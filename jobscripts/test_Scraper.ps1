



# cls
# # Define the log directory and validate it
# $logDir = "C:\Users\xnlou\OneDrive\Documents\Logs"
# try {
#     if (-not (Test-Path -Path $logDir)) {
#         New-Item -ItemType Directory -Path $logDir -Force -ErrorAction Stop | Out-Null
#         Write-Host "Created log directory: $logDir"
#     }
#     else {
#         Write-Host "Log directory already exists: $logDir"
#     }
# }
# catch {
#     Write-Host "Error creating log directory: $_"
#     exit 1
# }

# # Generate timestamp for consistent naming
# $timestamp = Get-Date -Format "yyyyMMddTHHmmss"

# # Generate CSV file name with timestamp
# $csvFile = Join-Path -Path $logDir -ChildPath "${timestamp}_MeetMaxURLCheck.csv"

# # Debugging: Output the CSV file path
# Write-Host "CSV file path: $csvFile"

# # Validate that the CSV file path is correctly formed
# if (-not $csvFile) {
#     Write-Host "Error: CSV file path could not be created."
#     exit 1
# }

# # Define the range of event numbers
# $start = 112600
# $end = 112700
# $total = $end - $start + 1
# $counter = 0

# # Initialize last progress update time
# $lastProgressUpdate = Get-Date

# # Initialize array to store results
# $results = @()

# # Loop through the range
# for ($i = $start; $i -le $end; $i++) {
#     $counter++
#     # Construct the URL
#     $url = "https://www.meetmax.com/sched/event_$i/__co-list_cp.html"
    
#     try {
#         # Send a web request using the GET method
#         $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 10 -MaximumRedirection 5 -ErrorAction Stop -Headers @{ "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" }
        
#         # Determine IfExists
#         $ifExists = if ($response.Content -like "*Invalid Event ID*") { 0 } else { 1 }
        
#         # Determine IsDownloadable and extract download URL
#         $isDownloadable = 0
#         $downloadUrl = ""
#         if ($response.Content -imatch '<a[^>]*class="[^"]*(download|excel)[^"]*"[^>]*>' -or $response.Content -imatch '<a[^>]*class="[^"]*link-excel[^"]*"[^>]*href="([^"]*__co-list_cp\.xls[^"]*)"[^>]*>') {
#             $isDownloadable = 1
#             if ($matches[1]) {
#                 $downloadUrl = "https://www.meetmax.com/sched/event_$i/" + $matches[1]
#             }
#         }
        
#         # Extract ConferenceName from <title> tag for valid events
#         $conferenceName = ""
#         if ($ifExists -eq 1) {
#             if ($response.Content -match "<title>(.*?)</title>") {
#                 $conferenceName = $matches[1] -replace " - MeetMax$", ""  # Clean up suffix
#             }
#         }
        
#         # Create result object
#         $result = [PSCustomObject]@{
#             EventID        = $i
#             URL            = $url
#             ConferenceName = $conferenceName
#             IfExists       = $ifExists
#             IsDownloadable = $isDownloadable
#         }
        
#         # Download the file if IsDownloadable = 1
#         if ($isDownloadable -eq 1 -and $downloadUrl) {
#             try {
#                 # Generate download filename
#                 $downloadFile = Join-Path -Path $logDir -ChildPath "${timestamp}_${i}.xls"
                
#                 # Download the file
#                 Invoke-WebRequest -Uri $downloadUrl -Method Get -TimeoutSec 10 -OutFile $downloadFile -ErrorAction Stop
#                 Write-Host "Successfully downloaded file: $downloadFile"
#             }
#             catch {
#                 Write-Host "Error downloading file for EventID $i from $downloadUrl : $_"
#             }
#         }
#     }
#     catch {
#         # Extract the status code from the error, if available
#         $statusCode = "Unknown"
#         if ($_.Exception.Response -ne $null) {
#             $statusCode = [int]$_.Exception.Response.StatusCode
#         }
        
#         # Create result object for failed request
#         $result = [PSCustomObject]@{
#             EventID        = $i
#             URL            = $url
#             ConferenceName = ""
#             IfExists       = 0
#             IsDownloadable = 0
#         }
#     }

#     # Add result to array
#     $results += $result

#     # Output to console
#     Write-Host "Processed URL: $url (EventID: $i, ConferenceName: $($result.ConferenceName), IfExists: $($result.IfExists), IsDownloadable: $($result.IsDownloadable))"

#     # Check if 10 seconds have passed since the last progress update
#     $currentTime = Get-Date
#     if (($currentTime - $lastProgressUpdate).TotalSeconds -ge 10) {
#         Write-Host "Processed $counter out of $total URLs"
#         $lastProgressUpdate = $currentTime
#     }

#     # Add a 1-second delay to avoid rate-limiting
#     Start-Sleep -Milliseconds 1000
# }

# # Export results to CSV
# try {
#     $results | Export-Csv -Path $csvFile -NoTypeInformation -ErrorAction Stop
#     Write-Host "Successfully wrote results to CSV file: $csvFile"
# }
# catch {
#     Write-Host "Error writing to CSV file: $_"
# }

# # Final progress update
# Write-Host "Completed: Processed $counter out of $total URLs"



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
    # Construct the public and private base URLs
    $publicUrl = "https://www.meetmax.com/sched/event_$i/__co-list_cp.html"
    $privateUrl = "https://www.meetmax.com/sched/event_$i/__private-co-list_cp.html"
    $urlsToTry = @($publicUrl, $privateUrl)
    $urlUsed = ""
    $isPrivate = $false
    $isDownloadable = 0
    $downloadUrl = ""
    $href = ""
    $ifExists = 0
    $conferenceName = ""
    
    Write-Host ("Starting processing for EventID {0}" -f $i)
    
    try {
        # Create a session to maintain cookies
        $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
        Write-Host ("Created new WebRequestSession for EventID {0}" -f $i)
        
        # Try each URL (public, then private)
        foreach ($url in $urlsToTry) {
            $urlUsed = $url
            $isPrivate = ($url -eq $privateUrl)
            Write-Host ("Trying {0} URL for EventID {1}: {2}" -f $(if ($isPrivate) { "private" } else { "public" }), $i, $url)
            
            try {
                Write-Host ("Fetching page for EventID {0} from {1}" -f $i, $url)
                $response = Invoke-WebRequest -Uri $url -Method Get -TimeoutSec 15 -MaximumRedirection 5 -WebSession $session -ErrorAction Stop -Headers @{ 
                    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
                    "Accept" = "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
                    "Accept-Encoding" = "gzip, deflate"
                }
                Write-Host ("Successfully fetched {0} page for EventID {1}, Status Code: {2}" -f $(if ($isPrivate) { "private" } else { "public" }), $i, $response.StatusCode)
                
                # Determine IfExists
                $ifExists = if ($response.Content -like "*Invalid Event ID*") { 0 } else { 1 }
                Write-Host ("IfExists for EventID {0} from {1} URL: {2}" -f $i, $(if ($isPrivate) { "private" } else { "public" }), $ifExists)
                
                # Check for downloadable link
                Write-Host ("Checking for downloadable link in HTML for EventID {0}" -f $i)
                if ($response.Content -imatch '<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>') {
                    $isDownloadable = 1
                    $href = $matches[1]
                    Write-Host ("Condition 1: Found downloadable link for EventID {0}, href: {1}" -f $i, $href)
                    if ($href -match '^https?://') {
                        Write-Host ("Subcondition 1a: href is a full URL for EventID {0}" -f $i)
                        $downloadUrl = $href
                        Write-Host ("Using full URL for EventID {0}: {1}" -f $i, $downloadUrl)
                    }
                    else {
                        Write-Host ("Subcondition 1b: href is relative for EventID {0}, appending to base URL" -f $i)
                        $baseUrl = "https://www.meetmax.com/sched/event_$i/"
                        $downloadUrl = $baseUrl + $href
                        Write-Host ("Constructed download URL for EventID {0} by appending href to base URL: {1}" -f $i, $downloadUrl)
                    }
                    break # Exit URL loop if link found
                }
                else {
                    Write-Host ("Condition 2: No downloadable link found in HTML for EventID {0} from {1} URL" -f $i, $(if ($isPrivate) { "public" } else { "private" }))
                    continue # Try next URL
                }
            }
            catch {
                Write-Host ("Failed to fetch {0} page for EventID {1}: {2}" -f $(if ($isPrivate) { "private" } else { "public" }), $i, $_.Exception.Message)
                continue # Try next URL
            }
        }
        
        # Extract ConferenceName from <title> tag for valid events
        if ($ifExists -eq 1) {
            Write-Host ("Extracting ConferenceName for EventID {0}" -f $i)
            if ($response.Content -match "<title>(.*?)</title>") {
                $conferenceName = $matches[1] -replace " - MeetMax$", ""
                Write-Host ("ConferenceName for EventID {0}: {1}" -f $i, $conferenceName)
            }
            else {
                Write-Host ("No ConferenceName found in <title> for EventID {0}" -f $i)
            }
        }
        
        # Create result object
        $result = [PSCustomObject]@{
            EventID        = $i
            URL            = $urlUsed
            ConferenceName = $conferenceName
            IfExists       = $ifExists
            IsDownloadable = $isDownloadable
        }
        Write-Host ("Created result object for EventID {0}: IfExists={1}, IsDownloadable={2}" -f $i, $ifExists, $isDownloadable)
        
        # Download the file if IsDownloadable = 1 and downloadUrl is non-empty
        if ($isDownloadable -eq 1 -and $downloadUrl) {
            Write-Host ("Preparing to download file for EventID {0} from {1}" -f $i, $downloadUrl)
            $retryCount = 0
            $maxRetries = 2
            $success = $false
            
            while (-not $success -and $retryCount -lt $maxRetries) {
                Write-Host ("Download attempt {0}/{1} for EventID {2}" -f ($retryCount + 1), $maxRetries, $i)
                try {
                    # Generate download filename
                    $downloadFile = Join-Path -Path $logDir -ChildPath "${timestamp}_${i}.xls"
                    Write-Host ("Download file path for EventID {0}: {1}" -f $i, $downloadFile)
                    
                    # Download the file with session and error checking
                    Write-Host ("Sending download request for EventID {0} to {1}" -f $i, $downloadUrl)
                    $downloadResponse = Invoke-WebRequest -Uri $downloadUrl -Method Get -TimeoutSec 15 -WebSession $session -ErrorAction Stop -Headers @{ 
                        "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
                        "Accept" = "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
                        "Accept-Encoding" = "gzip, deflate";
                        "Referer" = $urlUsed
                    }
                    Write-Host ("Received response for EventID {0}, Status Code: {1}" -f $i, $downloadResponse.StatusCode)
                    Write-Host ("Content length for EventID {0}: {1} bytes" -f $i, $downloadResponse.Content.Length)
                    Write-Host ("Raw Content-Type for EventID {0}: {1}" -f $i, $downloadResponse.Headers['Content-Type'])
                    if ($downloadResponse.StatusCode -eq 200 -and $downloadResponse.Content.Length -gt 1024) {
                        Write-Host ("Valid response for EventID {0}, saving file" -f $i)
                        [System.IO.File]::WriteAllBytes($downloadFile, $downloadResponse.Content)
                        Write-Host ("Successfully downloaded file for EventID {0}: {1}" -f $i, $downloadFile)
                        $success = $true
                    }
                    else {
                        Write-Host ("Invalid response for EventID {0}: Status {1}, Content Length {2}, Content-Type {3}" -f $i, $downloadResponse.StatusCode, $downloadResponse.Content.Length, $downloadResponse.Headers['Content-Type'])
                        Write-Host ("Response headers for EventID {0}: {1}" -f $i, ($downloadResponse.Headers | Out-String))
                        # Save response content for debugging
                        $debugFile = Join-Path -Path $logDir -ChildPath "${timestamp}_${i}_debug.html"
                        Write-Host ("Saving response content to debug file for EventID {0}: {1}" -f $i, $debugFile)
                        [System.IO.File]::WriteAllBytes($debugFile, $downloadResponse.Content)
                        $retryCount++
                    }
                }
                catch {
                    Write-Host ("Error during download for EventID {0} from {1}: {2}" -f $i, $downloadUrl, $_.Exception.Message)
                    if ($_.Exception.Response) {
                        Write-Host ("Download status code for EventID {0}: {1}" -f $i, $_.Exception.Response.StatusCode)
                        Write-Host ("Response headers for EventID {0}: {1}" -f $i, ($_.Exception.Response.Headers | Out-String))
                        if ($_.Exception.Response.StatusCode -in 403, 404, 429) {
                            $retryCount++
                            Write-Host ("Retrying download for EventID {0} (Attempt {1}/{2})" -f $i, $retryCount, $maxRetries)
                            # Re-fetch page to get fresh href
                            Write-Host ("Re-fetching page for EventID {0} to get fresh href from {1}" -f $i, $urlUsed)
                            $response = Invoke-WebRequest -Uri $urlUsed -Method Get -TimeoutSec 15 -MaximumRedirection 5 -WebSession $session -ErrorAction Stop -Headers @{ 
                                "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
                                "Accept" = "application/vnd.ms-excel,application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
                                "Accept-Encoding" = "gzip, deflate"
                            }
                            if ($response.Content -imatch '<a[^>]*href="([^"]*[_\-_]co-list_cp\.xls[^"]*)"[^>]*>') {
                                $href = $matches[1]
                                $downloadUrl = "https://www.meetmax.com/sched/event_$i/" + $href
                                Write-Host ("Re-fetched href for EventID {0}: {1}" -f $i, $href)
                                Write-Host ("Re-constructed download URL for EventID {0}: {1}" -f $i, $downloadUrl)
                            }
                            else {
                                Write-Host ("No new href found after re-fetch for EventID {0}" -f $i)
                            }
                            Start-Sleep -Milliseconds 4000
                            if ($retryCount -eq $maxRetries) {
                                Write-Host ("Max retries reached for EventID {0}. Skipping download." -f $i)
                            }
                            continue
                        }
                    }
                    Write-Host ("Non-retryable error for EventID {0}, aborting download" -f $i)
                    break
                }
            }
        }
        else {
            Write-Host ("Skipping download for EventID {0}: IsDownloadable={1}, downloadUrl={2}" -f $i, $isDownloadable, $downloadUrl)
        }
    }
    catch {
        # Extract the status code from the error, if available
        $statusCode = "Unknown"
        if ($_.Exception.Response -ne $null) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        
        # Create result object for failed request
        $result = [PSCustomObject]@{
            EventID        = $i
            URL            = $urlUsed
            ConferenceName = ""
            IfExists       = $ifExists
            IsDownloadable = 0
        }
        Write-Host ("Error processing URL for EventID {0}: Status {1} - {2}" -f $i, $statusCode, $_.Exception.Message)
    }

    # Add result to array
    $results += $result
    Write-Host ("Added result to CSV for EventID {0}" -f $i)

    # Output to console
    Write-Host ("Processed URL: {0} (EventID: {1}, ConferenceName: {2}, IfExists: {3}, IsDownloadable: {4})" -f $urlUsed, $i, $result.ConferenceName, $result.IfExists, $result.IsDownloadable)

    # Check if 10 seconds have passed since the last progress update
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

# Final progress update
Write-Host ("Completed: Processed {0} out of {1} URLs" -f $counter, $total)