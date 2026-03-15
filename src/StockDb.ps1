Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

function Get-ProjectRoot {
    return (Split-Path -Parent $PSScriptRoot)
}

function Resolve-ProjectPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }

    return [System.IO.Path]::GetFullPath((Join-Path (Get-ProjectRoot) $Path))
}

function Ensure-Directory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Read-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $content = Get-Content -LiteralPath $Path -Raw
    if ([string]::IsNullOrWhiteSpace($content)) {
        return $null
    }

    return $content | ConvertFrom-Json
}

function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        $Data,
        [int]$Depth = 20
    )

    Ensure-Directory -Path (Split-Path -Parent $Path)
    $json = $Data | ConvertTo-Json -Depth $Depth
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Write-TextFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    Ensure-Directory -Path (Split-Path -Parent $Path)
    [System.IO.File]::WriteAllText($Path, $Content, [System.Text.UTF8Encoding]::new($false))
}

function Read-Settings {
    $settingsPath = Join-Path (Get-ProjectRoot) "config\settings.json"
    if (-not (Test-Path -LiteralPath $settingsPath)) {
        $settingsPath = Join-Path (Get-ProjectRoot) "config\settings.example.json"
    }

    $settings = Read-JsonFile -Path $settingsPath
    if (-not $settings) {
        throw "Unable to load settings from $settingsPath"
    }

    $settings | Add-Member -NotePropertyName projectRoot -NotePropertyValue (Get-ProjectRoot) -Force
    $settings | Add-Member -NotePropertyName dataRootResolved -NotePropertyValue (Resolve-ProjectPath -Path $settings.dataRoot) -Force
    if (-not ($settings.PSObject.Properties.Name -contains "additionalCompaniesPath")) {
        $settings | Add-Member -NotePropertyName additionalCompaniesPath -NotePropertyValue (Join-Path (Get-ProjectRoot) "config\\additional_companies.json") -Force
    }
    return $settings
}

function Get-PathMap {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $dataRoot = $Settings.dataRootResolved
    $rawRoot = Join-Path $dataRoot "raw"
    $dbRoot = Join-Path $dataRoot "db"
    $announcementRoot = Join-Path $dataRoot "announcements"

    return [ordered]@{
        dataRoot             = $dataRoot
        rawRoot              = $rawRoot
        dbRoot               = $dbRoot
        announcementRoot     = $announcementRoot
        secRoot              = Join-Path $rawRoot "sec"
        companyRoot          = Join-Path (Join-Path $rawRoot "sec") "companies"
        companiesDb          = Join-Path $dbRoot "companies.json"
        filingsDb            = Join-Path $dbRoot "filings.json"
        annualFinancialsDb   = Join-Path $dbRoot "financials_annual.json"
        syncStateDb          = Join-Path $dbRoot "sync_state.json"
        sp500Snapshot        = Join-Path $dbRoot "sp500_constituents.json"
        latestAnnouncementMd = Join-Path $announcementRoot "latest.md"
        latestAnnouncementJs = Join-Path $announcementRoot "latest.json"
        historyRoot          = Join-Path $announcementRoot "history"
    }
}

function Get-RequestHeaders {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    return @{
        "User-Agent" = [string]$Settings.userAgent
        "Accept"     = "application/json, text/html, */*"
    }
}

function Invoke-RemoteText {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        $Settings
    )

    Start-Sleep -Milliseconds 200
    $headers = Get-RequestHeaders -Settings $Settings
    $output = & curl.exe --silent --show-error --location --compressed `
        --header ("User-Agent: " + $headers["User-Agent"]) `
        --header ("Accept: " + $headers["Accept"]) `
        $Url

    return [pscustomobject]@{
        Content = [string]$output
    }
}

function Invoke-RemoteJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        $Settings
    )

    Start-Sleep -Milliseconds 200
    $headers = Get-RequestHeaders -Settings $Settings
    $output = & curl.exe --silent --show-error --location --compressed `
        --header ("User-Agent: " + $headers["User-Agent"]) `
        --header ("Accept: " + $headers["Accept"]) `
        $Url

    if ([string]::IsNullOrWhiteSpace($output)) {
        throw "Empty response from $Url"
    }

    return ($output | ConvertFrom-Json)
}

function Normalize-Ticker {
    param(
        [AllowNull()]
        [string]$Ticker
    )

    if ([string]::IsNullOrWhiteSpace($Ticker)) {
        return $null
    }

    return $Ticker.Trim().ToUpperInvariant().Replace(".", "-")
}

function ConvertTo-Cik10 {
    param(
        [Parameter(Mandatory = $true)]
        $Value
    )

    $numeric = [string]$Value
    $numeric = $numeric.Trim()
    if ($numeric -match "^\d+$") {
        return $numeric.PadLeft(10, "0")
    }

    throw "Invalid CIK: $Value"
}

function ConvertTo-PlainObjectArray {
    param(
        [AllowNull()]
        $InputObject
    )

    if ($null -eq $InputObject) {
        return @()
    }

    if ($InputObject -is [System.Array]) {
        return @($InputObject)
    }

    return @($InputObject)
}

function Get-LookbackStartYear {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    return ([DateTime]::UtcNow.Year - [int]$Settings.lookbackYears + 1)
}

function Get-Sp500Constituents {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $response = Invoke-RemoteText -Url $Settings.sp500Source -Settings $Settings
    $html = [string]$response.Content
    $tableMatch = [regex]::Match($html, '<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>(.*?)</table>', [System.Text.RegularExpressions.RegexOptions]::Singleline)
    if (-not $tableMatch.Success) {
        throw "Unable to locate S&P 500 constituent table."
    }

    $rows = [regex]::Matches($tableMatch.Groups[1].Value, '<tr[^>]*>(.*?)</tr>', [System.Text.RegularExpressions.RegexOptions]::Singleline)
    $items = @()
    foreach ($row in $rows) {
        $cells = [regex]::Matches($row.Groups[1].Value, '<t[dh][^>]*>(.*?)</t[dh]>', [System.Text.RegularExpressions.RegexOptions]::Singleline)
        if ($cells.Count -lt 8) {
            continue
        }

        $symbol = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[0].Groups[1].Value, '<.*?>', '')).Trim())
        if ($symbol -eq "Symbol" -or [string]::IsNullOrWhiteSpace($symbol)) {
            continue
        }

        $security = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[1].Groups[1].Value, '<.*?>', '')).Trim())
        $gicsSector = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[3].Groups[1].Value, '<.*?>', '')).Trim())
        $gicsSubIndustry = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[4].Groups[1].Value, '<.*?>', '')).Trim())
        $headquarters = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[5].Groups[1].Value, '<.*?>', '')).Trim())
        $dateAdded = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[6].Groups[1].Value, '<.*?>', '')).Trim())
        $cik = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($cells[7].Groups[1].Value, '<.*?>', '')).Trim())

        $items += [pscustomobject]@{
            ticker          = (Normalize-Ticker -Ticker $symbol)
            security        = $security
            sector          = $gicsSector
            subIndustry     = $gicsSubIndustry
            headquarters    = $headquarters
            dateAdded       = $dateAdded
            cikFromSource   = $cik
        }
    }

    if ($items.Count -lt 400) {
        throw "Parsed constituent list is unexpectedly small: $($items.Count)"
    }

    return $items
}

function Get-SecTickerMap {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $payload = Invoke-RemoteJson -Url $Settings.secTickerMapUrl -Settings $Settings
    $items = @()

    foreach ($property in $payload.PSObject.Properties) {
        $row = $property.Value
        $items += [pscustomobject]@{
            ticker = (Normalize-Ticker -Ticker $row.ticker)
            title  = [string]$row.title
            cik    = (ConvertTo-Cik10 -Value $row.cik_str)
        }
    }

    return $items
}

function Get-AdditionalCompanies {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    if (-not (Test-Path -LiteralPath $Settings.additionalCompaniesPath)) {
        return @()
    }

    $payload = Read-JsonFile -Path $Settings.additionalCompaniesPath
    if (-not $payload) {
        return @()
    }

    $items = @()
    foreach ($row in (ConvertTo-PlainObjectArray -InputObject $payload)) {
        $ticker = Normalize-Ticker -Ticker $row.ticker
        if (-not $ticker) {
            continue
        }

        $security = if ($row.PSObject.Properties.Name -contains "security") { [string]$row.security } else { $ticker }
        $sector = if ($row.PSObject.Properties.Name -contains "sector") { [string]$row.sector } else { $null }
        $subIndustry = if ($row.PSObject.Properties.Name -contains "subIndustry") { [string]$row.subIndustry } else { $null }
        $headquarters = if ($row.PSObject.Properties.Name -contains "headquarters") { [string]$row.headquarters } else { $null }
        $dateAdded = if ($row.PSObject.Properties.Name -contains "dateAdded") { [string]$row.dateAdded } else { $null }
        $cikFromSource = if ($row.PSObject.Properties.Name -contains "cik") { [string]$row.cik } else { $null }

        $items += [pscustomobject]@{
            ticker        = $ticker
            security      = $security
            sector        = $sector
            subIndustry   = $subIndustry
            headquarters  = $headquarters
            dateAdded     = $dateAdded
            cikFromSource = $cikFromSource
        }
    }

    return $items
}

function Resolve-Companies {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $paths = Get-PathMap -Settings $Settings
    $constituents = @(
        Get-Sp500Constituents -Settings $Settings
        Get-AdditionalCompanies -Settings $Settings
    )
    $tickerMap = Get-SecTickerMap -Settings $Settings

    $mapByTicker = @{}
    foreach ($item in $tickerMap) {
        $mapByTicker[$item.ticker] = $item
    }

    $companies = @()
    $seenTickers = @{}
    foreach ($company in $constituents) {
        if ($seenTickers.ContainsKey($company.ticker)) {
            continue
        }
        $match = $null
        if ($mapByTicker.ContainsKey($company.ticker)) {
            $match = $mapByTicker[$company.ticker]
        } elseif ($company.ticker -like "*-*") {
            $alternate = $company.ticker.Replace("-", ".")
            if ($mapByTicker.ContainsKey($alternate)) {
                $match = $mapByTicker[$alternate]
            }
        }

        $resolvedCik = $null
        if ($match) {
            $resolvedCik = $match.cik
        } elseif ($company.cikFromSource -match "^\d+$") {
            $resolvedCik = ConvertTo-Cik10 -Value $company.cikFromSource
        }

        if (-not $resolvedCik) {
            continue
        }

        $companies += [pscustomobject]@{
            ticker       = $company.ticker
            cik          = $resolvedCik
            name         = if ($match) { $match.title } else { $company.security }
            security     = $company.security
            sector       = $company.sector
            subIndustry  = $company.subIndustry
            headquarters = $company.headquarters
            dateAdded    = $company.dateAdded
        }
        $seenTickers[$company.ticker] = $true
    }

    Write-JsonFile -Path $paths.sp500Snapshot -Data $companies
    Write-JsonFile -Path $paths.companiesDb -Data $companies
    return $companies
}

function Get-CompanyRoot {
    param(
        [Parameter(Mandatory = $true)]
        $Settings,
        [Parameter(Mandatory = $true)]
        [string]$Cik
    )

    $paths = Get-PathMap -Settings $Settings
    return (Join-Path $paths.companyRoot $Cik)
}

function Save-RemoteJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $payload = Invoke-RemoteJson -Url $Url -Settings $Settings
    Write-JsonFile -Path $Path -Data $payload
    return $payload
}

function Get-OrFetchJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        $Settings,
        [switch]$Force
    )

    if ((-not $Force) -and (Test-Path -LiteralPath $Path)) {
        return (Read-JsonFile -Path $Path)
    }

    return (Save-RemoteJson -Url $Url -Path $Path -Settings $Settings)
}

function Merge-SubmissionArrays {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.ArrayList]$Target,
        [Parameter(Mandatory = $true)]
        $Submission
    )

    $recent = $null
    if ($Submission.PSObject.Properties.Name -contains "filings" -and $Submission.filings) {
        $recent = $Submission.filings.recent
    } else {
        $recent = $Submission
    }

    if (-not $recent) {
        return
    }

    $accessionNumbers = ConvertTo-PlainObjectArray -InputObject $recent.accessionNumber
    $filingDates = ConvertTo-PlainObjectArray -InputObject $recent.filingDate
    $reportDates = ConvertTo-PlainObjectArray -InputObject $recent.reportDate
    $acceptanceDateTimes = ConvertTo-PlainObjectArray -InputObject $recent.acceptanceDateTime
    $forms = ConvertTo-PlainObjectArray -InputObject $recent.form
    $primaryDocuments = ConvertTo-PlainObjectArray -InputObject $recent.primaryDocument
    $primaryDocDescriptions = ConvertTo-PlainObjectArray -InputObject $recent.primaryDocDescription

    for ($i = 0; $i -lt $accessionNumbers.Count; $i++) {
        $Target.Add([pscustomobject]@{
            accessionNumber    = $accessionNumbers[$i]
            filingDate         = $filingDates[$i]
            reportDate         = $reportDates[$i]
            acceptanceDateTime = $acceptanceDateTimes[$i]
            form               = $forms[$i]
            primaryDocument    = $primaryDocuments[$i]
            description        = $primaryDocDescriptions[$i]
        }) | Out-Null
    }
}

function Get-AllSubmissionsForCompany {
    param(
        [Parameter(Mandatory = $true)]
        $Company,
        [Parameter(Mandatory = $true)]
        $Settings,
        [switch]$Force
    )

    $companyRoot = Get-CompanyRoot -Settings $Settings -Cik $Company.cik
    $submissionsRoot = Join-Path $companyRoot "submissions"
    Ensure-Directory -Path $submissionsRoot

    $primaryPath = Join-Path $submissionsRoot "CIK$($Company.cik).json"
    $primaryUrl = "$($Settings.secSubmissionsBaseUrl)/CIK$($Company.cik).json"
    $primary = Get-OrFetchJson -Url $primaryUrl -Path $primaryPath -Settings $Settings -Force:$Force

    $all = [System.Collections.ArrayList]::new()
    Merge-SubmissionArrays -Target $all -Submission $primary

    foreach ($file in (ConvertTo-PlainObjectArray -InputObject $primary.filings.files)) {
        if (-not $file.name) {
            continue
        }

        $historyPath = Join-Path $submissionsRoot $file.name
        $historyUrl = "$($Settings.secSubmissionsBaseUrl)/$($file.name)"
        $history = Get-OrFetchJson -Url $historyUrl -Path $historyPath -Settings $Settings -Force:$Force
        Merge-SubmissionArrays -Target $all -Submission $history
    }

    $seen = @{}
    $formsToTrack = @($Settings.formsToTrack)
    $filtered = @()
    foreach ($entry in $all) {
        if ($seen.ContainsKey($entry.accessionNumber)) {
            continue
        }

        $seen[$entry.accessionNumber] = $true
        if ($formsToTrack -notcontains [string]$entry.form) {
            continue
        }

        $filtered += [pscustomobject]@{
            cik                = $Company.cik
            ticker             = $Company.ticker
            companyName        = $Company.name
            accessionNumber    = $entry.accessionNumber
            filingDate         = $entry.filingDate
            reportDate         = $entry.reportDate
            acceptanceDateTime = $entry.acceptanceDateTime
            form               = $entry.form
            primaryDocument    = $entry.primaryDocument
            description        = $entry.description
        }
    }

    return ($filtered | Sort-Object filingDate, accessionNumber)
}

function Get-CompanyFacts {
    param(
        [Parameter(Mandatory = $true)]
        $Company,
        [Parameter(Mandatory = $true)]
        $Settings,
        [switch]$Force
    )

    $companyRoot = Get-CompanyRoot -Settings $Settings -Cik $Company.cik
    Ensure-Directory -Path $companyRoot

    $factsPath = Join-Path $companyRoot "companyfacts.json"
    $factsUrl = "$($Settings.secCompanyFactsBaseUrl)/CIK$($Company.cik).json"
    return (Get-OrFetchJson -Url $factsUrl -Path $factsPath -Settings $Settings -Force:$Force)
}

function New-ConceptMap {
    return [ordered]@{
        Revenue = @(
            @{ Taxonomy = "us-gaap"; Concept = "Revenues"; Unit = "USD" },
            @{ Taxonomy = "us-gaap"; Concept = "RevenueFromContractWithCustomerExcludingAssessedTax"; Unit = "USD" },
            @{ Taxonomy = "us-gaap"; Concept = "SalesRevenueNet"; Unit = "USD" }
        )
        NetIncome = @(
            @{ Taxonomy = "us-gaap"; Concept = "NetIncomeLoss"; Unit = "USD" }
        )
        OperatingIncome = @(
            @{ Taxonomy = "us-gaap"; Concept = "OperatingIncomeLoss"; Unit = "USD" }
        )
        TotalAssets = @(
            @{ Taxonomy = "us-gaap"; Concept = "Assets"; Unit = "USD" }
        )
        TotalLiabilities = @(
            @{ Taxonomy = "us-gaap"; Concept = "Liabilities"; Unit = "USD" }
        )
        ShareholdersEquity = @(
            @{ Taxonomy = "us-gaap"; Concept = "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"; Unit = "USD" },
            @{ Taxonomy = "us-gaap"; Concept = "StockholdersEquity"; Unit = "USD" }
        )
        CashAndEquivalents = @(
            @{ Taxonomy = "us-gaap"; Concept = "CashAndCashEquivalentsAtCarryingValue"; Unit = "USD" }
        )
        OperatingCashFlow = @(
            @{ Taxonomy = "us-gaap"; Concept = "NetCashProvidedByUsedInOperatingActivities"; Unit = "USD" }
        )
        CapitalExpenditure = @(
            @{ Taxonomy = "us-gaap"; Concept = "PaymentsToAcquirePropertyPlantAndEquipment"; Unit = "USD" }
        )
        DilutedEPS = @(
            @{ Taxonomy = "us-gaap"; Concept = "EarningsPerShareDiluted"; Unit = "USD/shares" }
        )
        SharesOutstanding = @(
            @{ Taxonomy = "dei"; Concept = "EntityCommonStockSharesOutstanding"; Unit = "shares" },
            @{ Taxonomy = "us-gaap"; Concept = "CommonStockSharesOutstanding"; Unit = "shares" }
        )
    }
}

function Get-FactEntries {
    param(
        [Parameter(Mandatory = $true)]
        $Facts,
        [Parameter(Mandatory = $true)]
        [string]$Taxonomy,
        [Parameter(Mandatory = $true)]
        [string]$Concept,
        [string]$Unit
    )

    if ($Facts.facts.PSObject.Properties.Name -notcontains $Taxonomy) {
        return @()
    }

    $taxonomyNode = $Facts.facts.$Taxonomy
    if ($taxonomyNode.PSObject.Properties.Name -notcontains $Concept) {
        return @()
    }

    $conceptNode = $taxonomyNode.$Concept
    if ($Unit) {
        return (ConvertTo-PlainObjectArray -InputObject $conceptNode.units.$Unit)
    }

    $items = @()
    foreach ($prop in $conceptNode.units.PSObject.Properties) {
        $items += ConvertTo-PlainObjectArray -InputObject $prop.Value
    }
    return $items
}

function Select-AnnualFact {
    param(
        [Parameter(Mandatory = $true)]
        $Entries,
        [Parameter(Mandatory = $true)]
        [int]$FiscalYear
    )

    $annualForms = @("10-K", "10-K/A", "10-KT")
    $candidates = @(
        $Entries |
            Where-Object {
                $_.fy -eq $FiscalYear -and
                $annualForms -contains [string]$_.form -and
                (
                    $_.PSObject.Properties.Name -notcontains "frame" -or
                    -not $_.frame
                )
            } |
            Sort-Object filed -Descending
    )

    if ($candidates.Count -eq 0) {
        $candidates = @(
            $Entries |
                Where-Object {
                    $_.fy -eq $FiscalYear -and
                    $annualForms -contains [string]$_.form
                } |
                Sort-Object filed -Descending
        )
    }

    if ($candidates.Count -eq 0) {
        return $null
    }

    return $candidates[0]
}

function Build-AnnualFinancialsForCompany {
    param(
        [Parameter(Mandatory = $true)]
        $Company,
        [Parameter(Mandatory = $true)]
        $Facts,
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $conceptMap = New-ConceptMap
    $startYear = Get-LookbackStartYear -Settings $Settings
    $endYear = [DateTime]::UtcNow.Year
    $rows = @()

    for ($year = $startYear; $year -le $endYear; $year++) {
        $row = [ordered]@{
            cik                 = $Company.cik
            ticker              = $Company.ticker
            companyName         = $Company.name
            fiscalYear          = $year
            Revenue             = $null
            NetIncome           = $null
            OperatingIncome     = $null
            TotalAssets         = $null
            TotalLiabilities    = $null
            ShareholdersEquity  = $null
            CashAndEquivalents  = $null
            OperatingCashFlow   = $null
            CapitalExpenditure  = $null
            FreeCashFlow        = $null
            DilutedEPS          = $null
            SharesOutstanding   = $null
            sourceFiledDate     = $null
            sourceForm          = $null
        }

        foreach ($metricName in $conceptMap.Keys) {
            $metricValue = $null
            $metricFiledDate = $null
            $metricForm = $null

            foreach ($candidate in $conceptMap[$metricName]) {
                $entries = Get-FactEntries -Facts $Facts -Taxonomy $candidate.Taxonomy -Concept $candidate.Concept -Unit $candidate.Unit
                if (@($entries).Count -eq 0) {
                    continue
                }

                $selected = Select-AnnualFact -Entries $entries -FiscalYear $year
                if ($selected) {
                    $metricValue = $selected.val
                    $metricFiledDate = $selected.filed
                    $metricForm = $selected.form
                    break
                }
            }

            $row[$metricName] = $metricValue
            if (-not $row.sourceFiledDate -and $metricFiledDate) {
                $row.sourceFiledDate = $metricFiledDate
            }
            if (-not $row.sourceForm -and $metricForm) {
                $row.sourceForm = $metricForm
            }
        }

        if ($row.OperatingCashFlow -ne $null -and $row.CapitalExpenditure -ne $null) {
            $row.FreeCashFlow = ([double]$row.OperatingCashFlow) - [math]::Abs([double]$row.CapitalExpenditure)
        }

        if ($row.sourceFiledDate) {
            $rows += [pscustomobject]$row
        }
    }

    return $rows
}

function Read-DbOrEmpty {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $data = Read-JsonFile -Path $Path
    if ($null -eq $data) {
        return @()
    }

    return (ConvertTo-PlainObjectArray -InputObject $data)
}

function Read-SyncState {
    param(
        [Parameter(Mandatory = $true)]
        $Settings
    )

    $paths = Get-PathMap -Settings $Settings
    $state = Read-JsonFile -Path $paths.syncStateDb
    if ($state) {
        return $state
    }

    return [pscustomobject]@{
        lastFullSyncUtc  = $null
        lastDailyRunUtc  = $null
        lastAnnouncement = $null
    }
}

function Write-SyncState {
    param(
        [Parameter(Mandatory = $true)]
        $Settings,
        [Parameter(Mandatory = $true)]
        $State
    )

    $paths = Get-PathMap -Settings $Settings
    Write-JsonFile -Path $paths.syncStateDb -Data $State
}

function Sync-CompanyData {
    param(
        [Parameter(Mandatory = $true)]
        $Company,
        [Parameter(Mandatory = $true)]
        $Settings,
        [switch]$Force
    )

    $facts = Get-CompanyFacts -Company $Company -Settings $Settings -Force:$Force
    $filings = Get-AllSubmissionsForCompany -Company $Company -Settings $Settings -Force:$Force
    $annuals = Build-AnnualFinancialsForCompany -Company $Company -Facts $facts -Settings $Settings

    return [pscustomobject]@{
        company = $Company
        filings = $filings
        annuals = $annuals
    }
}

function Save-Database {
    param(
        [Parameter(Mandatory = $true)]
        $Settings,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.ArrayList]$AllFilings,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.ArrayList]$AllAnnuals
    )

    $paths = Get-PathMap -Settings $Settings
    Write-JsonFile -Path $paths.filingsDb -Data @($AllFilings)
    Write-JsonFile -Path $paths.annualFinancialsDb -Data @($AllAnnuals)
}

function New-AnnouncementContent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Title,
        [Parameter(Mandatory = $true)]
        [string]$GeneratedAtUtc,
        [Parameter(Mandatory = $true)]
        $UpdatedCompanies,
        [Parameter(Mandatory = $true)]
        $NewFilings
    )

    $lines = @(
        "# $Title",
        "",
        "- GeneratedAtUtc: $GeneratedAtUtc",
        "- UpdatedCompanies: $($UpdatedCompanies.Count)",
        "- NewFilings: $($NewFilings.Count)",
        ""
    )

    if ($NewFilings.Count -gt 0) {
        $lines += "## New Filings"
        $lines += ""
        foreach ($filing in ($NewFilings | Sort-Object filingDate -Descending)) {
            $lines += "- $($filing.filingDate) | $($filing.ticker) | $($filing.form) | $($filing.accessionNumber)"
        }
        $lines += ""
    }

    if ($UpdatedCompanies.Count -gt 0) {
        $lines += "## Updated Companies"
        $lines += ""
        foreach ($company in ($UpdatedCompanies | Sort-Object ticker)) {
            $lines += "- $($company.ticker) | $($company.name) | CIK $($company.cik)"
        }
    }

    return ($lines -join [Environment]::NewLine)
}

function Publish-Announcement {
    param(
        [Parameter(Mandatory = $true)]
        $Settings,
        [Parameter(Mandatory = $true)]
        [string]$Title,
        [Parameter(Mandatory = $true)]
        $UpdatedCompanies,
        [Parameter(Mandatory = $true)]
        $NewFilings
    )

    $paths = Get-PathMap -Settings $Settings
    $generatedAt = [DateTime]::UtcNow.ToString("s") + "Z"
    $stamp = [DateTime]::UtcNow.ToString("yyyyMMdd-HHmmss")
    $content = New-AnnouncementContent -Title $Title -GeneratedAtUtc $generatedAt -UpdatedCompanies $UpdatedCompanies -NewFilings $NewFilings
    $payload = [pscustomobject]@{
        title            = $Title
        generatedAtUtc   = $generatedAt
        updatedCompanies = $UpdatedCompanies
        newFilings       = $NewFilings
    }

    Write-TextFile -Path $paths.latestAnnouncementMd -Content $content
    Write-JsonFile -Path $paths.latestAnnouncementJs -Data $payload
    Write-TextFile -Path (Join-Path $paths.historyRoot "$stamp.md") -Content $content
    Write-JsonFile -Path (Join-Path $paths.historyRoot "$stamp.json") -Data $payload
}

function Invoke-FullSync {
    param(
        [switch]$Force
    )

    $settings = Read-Settings
    $companies = Resolve-Companies -Settings $settings
    $allFilings = [System.Collections.ArrayList]::new()
    $allAnnuals = [System.Collections.ArrayList]::new()

    foreach ($company in $companies) {
        $result = Sync-CompanyData -Company $company -Settings $settings -Force:$Force
        foreach ($filing in $result.filings) {
            $allFilings.Add($filing) | Out-Null
        }
        foreach ($annual in $result.annuals) {
            $allAnnuals.Add($annual) | Out-Null
        }
    }

    Save-Database -Settings $settings -AllFilings $allFilings -AllAnnuals $allAnnuals

    $state = Read-SyncState -Settings $settings
    $state.lastFullSyncUtc = [DateTime]::UtcNow.ToString("s") + "Z"
    Write-SyncState -Settings $settings -State $state

    Publish-Announcement -Settings $settings -Title "Full Sync Completed" -UpdatedCompanies $companies -NewFilings @($allFilings)
}

function Group-AnnualsByTicker {
    param(
        [Parameter(Mandatory = $true)]
        $Annuals
    )

    $map = @{}
    foreach ($row in $Annuals) {
        if (-not $map.ContainsKey($row.ticker)) {
            $map[$row.ticker] = [System.Collections.ArrayList]::new()
        }

        $map[$row.ticker].Add($row) | Out-Null
    }

    return $map
}

function Group-FilingsByAccession {
    param(
        [Parameter(Mandatory = $true)]
        $Filings
    )

    $map = @{}
    foreach ($row in $Filings) {
        $map[$row.accessionNumber] = $row
    }
    return $map
}

function Invoke-DailyUpdate {
    $settings = Read-Settings
    $paths = Get-PathMap -Settings $settings
    $companies = Read-DbOrEmpty -Path $paths.companiesDb
    if ($companies.Count -eq 0) {
        $companies = Resolve-Companies -Settings $settings
    }

    $existingFilings = Read-DbOrEmpty -Path $paths.filingsDb
    $existingAnnuals = Read-DbOrEmpty -Path $paths.annualFinancialsDb
    $filingsByAccession = Group-FilingsByAccession -Filings $existingFilings
    $annualsByTicker = Group-AnnualsByTicker -Annuals $existingAnnuals

    $allFilings = [System.Collections.ArrayList]::new()
    foreach ($item in $existingFilings) {
        $allFilings.Add($item) | Out-Null
    }

    $updatedCompanies = @()
    $newFilings = @()

    foreach ($company in $companies) {
        $companyFilings = Get-AllSubmissionsForCompany -Company $company -Settings $settings
        $hasNew = $false

        foreach ($filing in $companyFilings) {
            if (-not $filingsByAccession.ContainsKey($filing.accessionNumber)) {
                $filingsByAccession[$filing.accessionNumber] = $filing
                $allFilings.Add($filing) | Out-Null
                $newFilings += $filing
                $hasNew = $true
            }
        }

        if ($hasNew) {
            $facts = Get-CompanyFacts -Company $company -Settings $settings -Force
            $annualsByTicker[$company.ticker] = [System.Collections.ArrayList]::new()
            foreach ($row in (Build-AnnualFinancialsForCompany -Company $company -Facts $facts -Settings $settings)) {
                $annualsByTicker[$company.ticker].Add($row) | Out-Null
            }
            $updatedCompanies += $company
        }
    }

    if ($newFilings.Count -gt 0) {
        $flattenedAnnuals = [System.Collections.ArrayList]::new()
        foreach ($ticker in $annualsByTicker.Keys) {
            foreach ($row in $annualsByTicker[$ticker]) {
                $flattenedAnnuals.Add($row) | Out-Null
            }
        }

        Save-Database -Settings $settings -AllFilings $allFilings -AllAnnuals $flattenedAnnuals
        Publish-Announcement -Settings $settings -Title "Daily SEC Update" -UpdatedCompanies $updatedCompanies -NewFilings $newFilings
    } else {
        Publish-Announcement -Settings $settings -Title "Daily SEC Update - No Changes" -UpdatedCompanies @() -NewFilings @()
    }

    $state = Read-SyncState -Settings $settings
    $state.lastDailyRunUtc = [DateTime]::UtcNow.ToString("s") + "Z"
    $state.lastAnnouncement = $paths.latestAnnouncementJs
    Write-SyncState -Settings $settings -State $state
}

function Register-DailyTask {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DailyTime
    )

    $projectRoot = Get-ProjectRoot
    $scriptPath = Join-Path $projectRoot "scripts\Update-Daily.ps1"
    $taskName = "StockSecDailyUpdate"
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$scriptPath`""
    $trigger = New-ScheduledTaskTrigger -Daily -At $DailyTime
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Description "Daily SEC filing update for S&P 500 database" -Force | Out-Null
}
