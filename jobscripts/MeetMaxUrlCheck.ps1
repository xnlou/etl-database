cls
# Define the log directory and validate it
$logDir = "C:\Users\xnlou\OneDrive\Documents\Logs"
try {
    if (-not (Test-Path -Path $logDir)) {
        New-Item -ItemType Directory -Path $logDir -Force -ErrorAction Stop | Out-Null
        Write-Host "Created log directory: $logDir"
    }
    else {
        Write-Host "Log directory already exists: $logDir"
    }
}
catch {
    Write-Host "Error creating log directory: $_"
    exit 1
}

# Generate timestamp for consistent naming
$timestamp = Get-Date -Format "yyyyMMddTHHmmss"

# Generate CSV file name with timestamp
$csvFile = Join-Path -Path $logDir -ChildPath "${timestamp}_MeetMaxURLCheck.csv"

# Debugging: Output the CSV file path
Write-Host "CSV file path: $csvFile"

# Validate that the CSV file path is correctly formed
if (-not $csvFile) {
    Write-Host "Error: CSV file path could not be created."
    exit 1
}

# Define the specific event IDs to process
$eventIds = @(112601, 112621, 112643, 112657, 112663, 112687, 112691)
$total = $eventIds.Count
$counter = 0

# Initialize last progress update time
$lastProgressUpdate = Get-Date

# Initialize array to store results
$results = @()

# Process each event ID
foreach ($i in $eventIds) {
    $counter++
    Write-Host "---"
    Write-Host ("Starting processing for EventID {0}" -f $i)
    
    # Construct the public and private URLs
    $publicUrl = "https://www.meetmax.com/sched/event_$i/__co-list_cp.html"
    $privateUrl = "https://www.meetmax.com/sched/event_$i/__private-co-list_cp.html"
    $urlUsed = ""
    $isPrivate = $false
    $isDownloadable = 0
    $downloadLink = ""
    $ifExists = 0
    $conferenceName = ""
    
    try {
        # Create a session to maintain cookies
        $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
        Write-Host ("Created new WebRequestSession for EventID {0}" -f $i)
        
        # Check public page for private site indicator
        Write-Host ("Checking public page for private site indicator: {0}" -f $publicUrl)
        $publicResponse = Invoke-WebRequest -Uri $publicUrl -Method Get -TimeoutSec 15 -MaximumRedirection 5 -WebSession $session -ErrorAction Stop -Headers @{ 
            "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
            "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
            "Accept-Encoding" = "gzip, deflate"
        }
        Write-Host ("Successfully fetched public page for EventID {0}, Status Code: {1}" -f $i, $publicResponse.StatusCode)
        
        # Look for private site link
        if ($publicResponse.Content -imatch '<a[^>]*href="[^"]*__private-co-list_cp\.html[^"]*"[^>]*class="[^"]*nav-link[^"]*"[^>]*>Private Company List</a>') {
            Write-Host ("Private site indicator found for EventID {0} in public page HTML" -f $i)
            $isPrivate = $true
        }
        else {
            Write-Host ("No private site indicator found for EventID {0} in public page HTML" -f $i)
        }
        
        # Select URL based on private site check
        $urlUsed = if ($isPrivate) { $privateUrl } else { $publicUrl }
        Write-Host ("Using {0} URL for EventID {1}: {2}" -f $(if ($isPrivate) { "private" } else { "public" }), $i, $urlUsed)
        
        # Fetch the selected page (private if indicated, else public)
        Write-Host ("Fetching page for EventID {0} from {1}" -f $i, $urlUsed)
        $response = $publicResponse
        if ($isPrivate) {
            $response = Invoke-WebRequest -Uri $privateUrl -Method Get -TimeoutSec 15 -MaximumRedirection 5 -WebSession $session -ErrorAction Stop -Headers @{ 
                "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
                "Accept" = "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
                "Accept-Encoding" = "gzip, deflate"
            }
            Write-Host ("Successfully fetched private page for EventID {0}, Status Code: {1}" -f $i, $response.StatusCode)
        }
        else {
            Write-Host ("Using already fetched public page for EventID {0}" -f $i)
        }
        
        # Determine IfExists
        $ifExists = if ($response.Content -like "*Invalid Event ID*") { 0 } else { 1 }
        Write-Host ("IfExists for EventID {0}: {1}" -f $i, $ifExists)
        
        # Check for downloadable link
        Write-Host ("Checking for downloadable link in HTML for EventID {0}" -f $i)
        if ($response.Content -imatch '<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>') {
            $isDownloadable = 1
            $href = $matches[1]
            Write-Host ("Condition 1: Found downloadable link for EventID {0}, href: {1}" -f $i, $href)
            if ($href -match '^https?://') {
                Write-Host ("Subcondition 1a: href is a full URL for EventID {0}" -f $i)
                $downloadLink = $href
                Write-Host ("Using full URL for EventID {0}: {1}" -f $i, $downloadLink)
            }
            else {
                Write-Host ("Subcondition 1b: href is relative for EventID {0}, appending to base URL" -f $i)
                $baseUrl = "https://www.meetmax.com/sched/event_$i/"
                $downloadLink = $baseUrl + $href
                Write-Host ("Constructed download URL for EventID {0}: {1}" -f $i, $downloadLink)
            }
        }
        else {
            Write-Host ("Condition 2: No downloadable link found in HTML for EventID {0}" -f $i)
        }
        
        # Extract ConferenceName
        if ($ifExists -eq 1) {
            Write-Host ("Extracting ConferenceName for EventID {0}" -f $i)
            if ($response.Content -match "<title>(.*?)</title>") {
                $conferenceName = $matches[1] -replace " - MeetMax$", ""
                Write-Host ("ConferenceName for EventID {0}: {1}" -f $i, $conferenceName)
            }
            else {
                Write-Host ("No ConferenceName found for EventID {0}" -f $i)
            }
        }
        
        # Create result object
        $result = [PSCustomObject]@{
            EventID        = $i
            URL            = $urlUsed
            ConferenceName = $conferenceName
            IfExists       = $ifExists
            IsDownloadable = $isDownloadable
            DownloadLink   = $downloadLink
        }
        Write-Host ("Created result object for EventID {0}: IfExists={1}, IsDownloadable={2}, DownloadLink={3}" -f $i, $ifExists, $isDownloadable, $downloadLink)
    }
    catch {
        $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "Unknown" }
        $result = [PSCustomObject]@{
            EventID        = $i
            URL            = $urlUsed
            ConferenceName = ""
            IfExists       = 0
            IsDownloadable = 0
            DownloadLink   = ""
        }
        Write-Host ("Error processing EventID {0}: Status {1} - {2}" -f $i, $statusCode, $_.Exception.Message)
    }

    # Add result to array
    $results += $result
    
    # Progress update every 10 seconds
    $currentTime = Get-Date
    if (($currentTime - $lastProgressUpdate).TotalSeconds -ge 10) {
        Write-Host ("Processed {0} out of {1} URLs" -f $counter, $total)
        $lastProgressUpdate = $currentTime
    }
    
    # Add a 4-second delay to avoid rate-limiting
    Write-Host ("Pausing for 4 seconds before next EventID" -f $i)
    Start-Sleep -Milliseconds 4000
}

# Export results to CSV
try {
    Write-Host "Exporting results to CSV: $csvFile"
    $results | Export-Csv -Path $csvFile -NoTypeInformation -ErrorAction Stop
    Write-Host "Successfully wrote results to CSV file: $csvFile"
}
catch {
    Write-Host "Error writing to CSV file: $_"
}

Write-Host ("Completed: Processed {0} out of {1} URLs" -f $counter, $total)