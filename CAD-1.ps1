<#
.SYNOPSIS
    Discover Azure Government VMs and export a CSV with performance metrics,
    tags, update status, and a resource link.

.DESCRIPTION
    Output CSV columns:
      Server Name
      Subscription Name
      Resource Group Name
      Tags
      Percentage CPU (Avg)
      Percentage CPU (Max)
      Available Memory Percentage (Avg)
      Available Memory Percentage (Min)
      OS Disk IOPS Consumed Percentage (Avg)
      Network In Total (Sum)
      Network Out Total (Sum)
      Update Status
      Resource Link

.NOTES
    Required modules:
      Az.Accounts
      Az.Compute
      Az.Monitor
      Az.ResourceGraph

    Notes:
      - Available Memory Percentage may be blank unless guest metrics are available.
      - OS Disk IOPS Consumed Percentage may be blank on VMs/SKUs that do not expose it.
      - Update Status is derived from the latest Azure Update Manager assessment record
        found in Azure Resource Graph for the VM.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$OutputCsv = ".\AzureGov-VM-MonthlyMetrics.csv",

    [Parameter(Mandatory = $false)]
    [switch]$UseDeviceAuthentication,

    [Parameter(Mandatory = $false)]
    [string[]]$SubscriptionId
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

function Convert-TagsToString {
    param(
        [Parameter(Mandatory = $false)]
        [hashtable]$Tags
    )

    if (-not $Tags -or $Tags.Count -eq 0) {
        return ""
    }

    $pairs = foreach ($key in ($Tags.Keys | Sort-Object)) {
        $value = $Tags[$key]
        "{0}={1}" -f $key, $value
    }

    return ($pairs -join "; ")
}

function Get-AzureGovResourceLink {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResourceId
    )

    return "https://portal.azure.us/#resource$ResourceId/overview"
}

function Get-MetricAggregateValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ResourceId,

        [Parameter(Mandatory = $true)]
        [string]$MetricName,

        [Parameter(Mandatory = $true)]
        [datetime]$StartTimeUtc,

        [Parameter(Mandatory = $true)]
        [datetime]$EndTimeUtc,

        [Parameter(Mandatory = $true)]
        [ValidateSet("Average","Minimum","Maximum","Total","Count")]
        [string]$AggregationType,

        [Parameter(Mandatory = $true)]
        [ValidateSet("Average","Minimum","Maximum","Sum")]
        [string]$Rollup
    )

    try {
        $metric = Get-AzMetric `
            -ResourceId $ResourceId `
            -MetricName $MetricName `
            -StartTime $StartTimeUtc `
            -EndTime $EndTimeUtc `
            -TimeGrain 01:00:00 `
            -AggregationType $AggregationType `
            -ErrorAction Stop

        if (-not $metric -or -not $metric.Data) {
            return $null
        }

        switch ($AggregationType) {
            "Average" { $values = @($metric.Data | Where-Object { $null -ne $_.Average } | Select-Object -ExpandProperty Average) }
            "Minimum" { $values = @($metric.Data | Where-Object { $null -ne $_.Minimum } | Select-Object -ExpandProperty Minimum) }
            "Maximum" { $values = @($metric.Data | Where-Object { $null -ne $_.Maximum } | Select-Object -ExpandProperty Maximum) }
            "Total"   { $values = @($metric.Data | Where-Object { $null -ne $_.Total }   | Select-Object -ExpandProperty Total) }
            "Count"   { $values = @($metric.Data | Where-Object { $null -ne $_.Count }   | Select-Object -ExpandProperty Count) }
            default   { $values = @() }
        }

        if ($values.Count -eq 0) {
            return $null
        }

        switch ($Rollup) {
            "Average" { return [math]::Round((($values | Measure-Object -Average).Average), 2) }
            "Minimum" { return [math]::Round((($values | Measure-Object -Minimum).Minimum), 2) }
            "Maximum" { return [math]::Round((($values | Measure-Object -Maximum).Maximum), 2) }
            "Sum"     { return [math]::Round((($values | Measure-Object -Sum).Sum), 2) }
        }
    }
    catch {
        return $null
    }
}

function Invoke-AzGraphQueryAll {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Query,

        [Parameter(Mandatory = $false)]
        [string[]]$SubscriptionIds
    )

    $allRows = New-Object System.Collections.Generic.List[object]
    $skipToken = $null

    do {
        $params = @{
            Query = $Query
            First = 1000
        }

        if ($SubscriptionIds -and $SubscriptionIds.Count -gt 0) {
            $params["Subscription"] = $SubscriptionIds
        }

        if ($skipToken) {
            $params["SkipToken"] = $skipToken
        }

        $response = Search-AzGraph @params

        if ($response.Data) {
            foreach ($row in $response.Data) {
                $allRows.Add($row)
            }
        }

        $skipToken = $response.SkipToken
    }
    while ($skipToken)

    return $allRows
}

function Get-UpdateStatusMap {
    param(
        [Parameter(Mandatory = $false)]
        [string[]]$SubscriptionIds
    )

    $query = @"
patchassessmentresources
| where type !has "softwarepatches"
| extend vmResourceId = tostring(split(id, '/patchAssessmentResults/', 0))
| extend prop = parse_json(properties)
| extend lastModified = todatetime(prop.lastModifiedDateTime)
| extend rebootPending = tostring(prop.rebootPending)
| extend availablePatchCountByClassification = prop.availablePatchCountByClassification
| extend availablePatchCount =
    toint(coalesce(availablePatchCountByClassification.critical, 0)) +
    toint(coalesce(availablePatchCountByClassification.security, 0)) +
    toint(coalesce(availablePatchCountByClassification.updateRollup, 0)) +
    toint(coalesce(availablePatchCountByClassification.featurePack, 0)) +
    toint(coalesce(availablePatchCountByClassification.servicePack, 0)) +
    toint(coalesce(availablePatchCountByClassification.definition, 0)) +
    toint(coalesce(availablePatchCountByClassification.updates, 0)) +
    toint(coalesce(availablePatchCountByClassification.tools, 0)) +
    toint(coalesce(availablePatchCountByClassification.other, 0))
| summarize arg_max(lastModified, *) by vmResourceId
| project vmResourceId, lastModified, rebootPending, availablePatchCount
"@

    $rows = Invoke-AzGraphQueryAll -Query $query -SubscriptionIds $SubscriptionIds
    $map = @{}

    foreach ($row in $rows) {
        $status = "No assessment data"

        $availablePatchCount = 0
        if ($null -ne $row.availablePatchCount -and "$($row.availablePatchCount)" -ne "") {
            $availablePatchCount = [int]$row.availablePatchCount
        }

        $rebootPending = "$($row.rebootPending)"

        if ($rebootPending -eq "true" -or $rebootPending -eq "True") {
            if ($availablePatchCount -gt 0) {
                $status = "Updates available ($availablePatchCount); Reboot pending"
            }
            else {
                $status = "Reboot pending"
            }
        }
        elseif ($availablePatchCount -gt 0) {
            $status = "Updates available ($availablePatchCount)"
        }
        else {
            $status = "Up to date"
        }

        $map[$row.vmResourceId.ToLowerInvariant()] = $status
    }

    return $map
}

try {
    Test-RequiredModule -Name "Az.Accounts"
    Test-RequiredModule -Name "Az.Compute"
    Test-RequiredModule -Name "Az.Monitor"
    Test-RequiredModule -Name "Az.ResourceGraph"

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

    $subscriptionIds = @($subscriptions | Select-Object -ExpandProperty Id)
    Write-Host "Loading update assessment status from Azure Resource Graph..." -ForegroundColor Cyan
    $updateStatusMap = Get-UpdateStatusMap -SubscriptionIds $subscriptionIds

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

            $resourceId = $vm.Id
            $resourceIdKey = $resourceId.ToLowerInvariant()

            $cpuAvg = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Percentage CPU" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Average" `
                -Rollup "Average"

            $cpuMax = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Percentage CPU" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Maximum" `
                -Rollup "Maximum"

            $memAvg = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Available Memory Percentage" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Average" `
                -Rollup "Average"

            $memMin = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Available Memory Percentage" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Minimum" `
                -Rollup "Minimum"

            $osDiskIopsAvg = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "OS Disk IOPS Consumed Percentage" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Average" `
                -Rollup "Average"

            $networkInSum = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Network In Total" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Total" `
                -Rollup "Sum"

            $networkOutSum = Get-MetricAggregateValue `
                -ResourceId $resourceId `
                -MetricName "Network Out Total" `
                -StartTimeUtc $window.StartUtc `
                -EndTimeUtc $window.EndUtc `
                -AggregationType "Total" `
                -Rollup "Sum"

            $updateStatus = if ($updateStatusMap.ContainsKey($resourceIdKey)) {
                $updateStatusMap[$resourceIdKey]
            }
            else {
                "No assessment data"
            }

            $results.Add([PSCustomObject]@{
                "Server Name"                           = $vm.Name
                "Subscription Name"                     = $sub.Name
                "Resource Group Name"                   = $vm.ResourceGroupName
                "Tags"                                  = Convert-TagsToString -Tags $vm.Tags
                "Percentage CPU (Avg)"                  = $cpuAvg
                "Percentage CPU (Max)"                  = $cpuMax
                "Available Memory Percentage (Avg)"     = $memAvg
                "Available Memory Percentage (Min)"     = $memMin
                "OS Disk IOPS Consumed Percentage (Avg)"= $osDiskIopsAvg
                "Network In Total (Sum)"                = $networkInSum
                "Network Out Total (Sum)"               = $networkOutSum
                "Update Status"                         = $updateStatus
                "Resource Link"                         = Get-AzureGovResourceLink -ResourceId $resourceId
            })
        }
    }

    $finalResults = $results | Sort-Object "Subscription Name", "Resource Group Name", "Server Name"

    $finalResults | Export-Csv -LiteralPath $OutputCsv -NoTypeInformation -Encoding UTF8

    $finalResults |
        Select-Object "Server Name","Subscription Name","Resource Group Name","Percentage CPU (Avg)","Percentage CPU (Max)","Update Status" |
        Format-Table -AutoSize

    Write-Host ""
    Write-Host ("Report exported to: {0}" -f $OutputCsv) -ForegroundColor Cyan
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
