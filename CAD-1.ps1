<#
.SYNOPSIS
    Discover all Azure VMs in Azure Government and report previous month's
    average and peak CPU utilization.

.DESCRIPTION
    - Connects to Azure Government
    - Enumerates all accessible subscriptions
    - Enumerates all VMs in each subscription
    - Pulls Azure Monitor "Percentage CPU" for the previous calendar month
    - Outputs results to screen and CSV

.NOTES
    Required modules:
      Az.Accounts
      Az.Compute
      Az.Monitor
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$OutputCsv = ".\AzureGov-PreviousMonth-CPUReport.csv",

    [Parameter(Mandatory = $false)]
    [switch]$UseDeviceAuthentication,

    [Parameter(Mandatory = $false)]
    [string[]]$SubscriptionId,

    [Parameter(Mandatory = $false)]
    [switch]$IncludeStoppedVMs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-RequiredModule {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (-not (Get-Module -ListAvailable -Name $Name)) {
        throw "Required module '$Name' is not installed. Install it with: Install-Module $Name -Scope CurrentUser"
    }
}

function Get-PreviousMonthWindowUtc {
    $now = Get-Date
    $firstDayOfCurrentMonth = Get-Date -Year $now.Year -Month $now.Month -Day 1 -Hour 0 -Minute 0 -Second 0
    $startLocal = $firstDayOfCurrentMonth.AddMonths(-1)
    $endLocal   = $firstDayOfCurrentMonth.AddSeconds(-1)

    [PSCustomObject]@{
        StartLocal = $startLocal
        EndLocal   = $endLocal
        StartUtc   = $startLocal.ToUniversalTime()
        EndUtc     = $endLocal.ToUniversalTime()
        Label      = $startLocal.ToString("yyyy-MM")
    }
}

function Get-VmPowerState {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Vm
    )

    $statusesProperty = $Vm.PSObject.Properties["Statuses"]

    if ($statusesProperty -and $statusesProperty.Value) {
        $power = $statusesProperty.Value |
            Where-Object { $_.Code -like "PowerState/*" } |
            Select-Object -ExpandProperty DisplayStatus -First 1

        if ($power) {
            return $power
        }
    }

    return "Unknown"
}

function Get-OverallCpuStats {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResourceId,

        [Parameter(Mandatory = $true)]
        [datetime]$StartTimeUtc,

        [Parameter(Mandatory = $true)]
        [datetime]$EndTimeUtc
    )

    $metric = Get-AzMetric `
        -ResourceId $ResourceId `
        -MetricName "Percentage CPU" `
        -StartTime $StartTimeUtc `
        -EndTime $EndTimeUtc `
        -TimeGrain 01:00:00 `
        -DetailedOutput `
        -ErrorAction Stop

    if (-not $metric -or -not $metric.Data) {
        return [PSCustomObject]@{
            AverageCpu = $null
            PeakCpu    = $null
            Samples    = 0
        }
    }

    $avgValues = @(
        $metric.Data |
        Where-Object { $null -ne $_.Average } |
        Select-Object -ExpandProperty Average
    )

    $maxValues = @(
        $metric.Data |
        Where-Object { $null -ne $_.Maximum } |
        Select-Object -ExpandProperty Maximum
    )

    $overallAverage = $null
    if ($avgValues.Count -gt 0) {
        $overallAverage = [math]::Round((($avgValues | Measure-Object -Average).Average), 2)
    }

    $overallPeak = $null
    if ($maxValues.Count -gt 0) {
        $overallPeak = [math]::Round((($maxValues | Measure-Object -Maximum).Maximum), 2)
    }

    [PSCustomObject]@{
        AverageCpu = $overallAverage
        PeakCpu    = $overallPeak
        Samples    = [math]::Max($avgValues.Count, $maxValues.Count)
    }
}

try {
    Test-RequiredModule -Name "Az.Accounts"
    Test-RequiredModule -Name "Az.Compute"
    Test-RequiredModule -Name "Az.Monitor"

    Write-Host "Connecting to Azure Government..." -ForegroundColor Cyan
    if ($UseDeviceAuthentication) {
        Connect-AzAccount -Environment AzureUSGovernment -UseDeviceAuthentication | Out-Null
    }
    else {
        Connect-AzAccount -Environment AzureUSGovernment | Out-Null
    }

    $window = Get-PreviousMonthWindowUtc

    Write-Host "Reporting window:" -ForegroundColor Cyan
    Write-Host ("  Local: {0} through {1}" -f $window.StartLocal, $window.EndLocal)
    Write-Host ("  UTC:   {0} through {1}" -f $window.StartUtc, $window.EndUtc)

    $subscriptions = if ($SubscriptionId -and $SubscriptionId.Count -gt 0) {
        Get-AzSubscription | Where-Object { $_.Id -in $SubscriptionId }
    }
    else {
        Get-AzSubscription
    }

    if (-not $subscriptions) {
        throw "No accessible subscriptions were found."
    }

    $results = New-Object System.Collections.Generic.List[object]

    foreach ($sub in $subscriptions) {
        Write-Host ""
        Write-Host ("Subscription: {0} ({1})" -f $sub.Name, $sub.Id) -ForegroundColor Yellow

        Set-AzContext -SubscriptionId $sub.Id | Out-Null

        $vms = Get-AzVM

        if (-not $vms) {
            Write-Host "  No VMs found." -ForegroundColor DarkGray
            continue
        }

        foreach ($vm in $vms) {
            Write-Host ("  Processing {0}..." -f $vm.Name) -ForegroundColor Green

            try {
                $vmWithStatus = Get-AzVM `
                    -ResourceGroupName $vm.ResourceGroupName `
                    -Name $vm.Name `
                    -Status `
                    -ErrorAction Stop

                $powerState = Get-VmPowerState -Vm $vmWithStatus

                if (-not $IncludeStoppedVMs -and $powerState -ne "VM running" -and $powerState -ne "Unknown") {
                    Write-Host ("  Skipping {0} ({1})" -f $vm.Name, $powerState) -ForegroundColor DarkGray

                    $results.Add([PSCustomObject]@{
                        Month             = $window.Label
                        SubscriptionName  = $sub.Name
                        SubscriptionId    = $sub.Id
                        ResourceGroupName = $vm.ResourceGroupName
                        VMName            = $vm.Name
                        Location          = $vm.Location
                        VMSize            = $vm.HardwareProfile.VmSize
                        PowerState        = $powerState
                        AverageCpuPct     = $null
                        PeakCpuPct        = $null
                        SampleCount       = 0
                        Status            = "Skipped"
                        Error             = "VM not running"
                    })

                    continue
                }

                $stats = Get-OverallCpuStats `
                    -ResourceId $vm.Id `
                    -StartTimeUtc $window.StartUtc `
                    -EndTimeUtc $window.EndUtc

                $results.Add([PSCustomObject]@{
                    Month             = $window.Label
                    SubscriptionName  = $sub.Name
                    SubscriptionId    = $sub.Id
                    ResourceGroupName = $vm.ResourceGroupName
                    VMName            = $vm.Name
                    Location          = $vm.Location
                    VMSize            = $vm.HardwareProfile.VmSize
                    PowerState        = $powerState
                    AverageCpuPct     = $stats.AverageCpu
                    PeakCpuPct        = $stats.PeakCpu
                    SampleCount       = $stats.Samples
                    Status            = "OK"
                    Error             = $null
                })
            }
            catch {
                $results.Add([PSCustomObject]@{
                    Month             = $window.Label
                    SubscriptionName  = $sub.Name
                    SubscriptionId    = $sub.Id
                    ResourceGroupName = $vm.ResourceGroupName
                    VMName            = $vm.Name
                    Location          = $vm.Location
                    VMSize            = $vm.HardwareProfile.VmSize
                    PowerState        = "Unknown"
                    AverageCpuPct     = $null
                    PeakCpuPct        = $null
                    SampleCount       = 0
                    Status            = "Failed"
                    Error             = $_.Exception.Message
                })

                Write-Warning ("Failed to process {0}: {1}" -f $vm.Name, $_.Exception.Message)
            }
        }
    }

    $finalResults = $results | Sort-Object SubscriptionName, ResourceGroupName, VMName

    $finalResults | Format-Table -AutoSize

    $finalResults |
        Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding UTF8

    Write-Host ""
    Write-Host ("Report exported to: {0}" -f $OutputCsv) -ForegroundColor Cyan
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}