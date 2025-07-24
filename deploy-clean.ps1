# UK Energy Grid Tracker - Clean Deployment Script
# This script deploys the clean grid tracker to the live server

# Load configuration
$configFile = "deploy-config.local"
if (-not (Test-Path $configFile)) {
    Write-Host "[ERROR] Configuration file '$configFile' not found!" -ForegroundColor Red
    Write-Host "Please create this file with your server details." -ForegroundColor Red
    exit 1
}

# Parse config file manually to handle KEY=value format
$config = @{}
Get-Content $configFile | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $config[$matches[1]] = $matches[2]
    }
}

$DropletIP = $config.DROPLET_IP
$RootPassword = $config.ROOT_PASSWORD

if (-not $DropletIP -or -not $RootPassword) {
    Write-Host "[ERROR] Missing DROPLET_IP or ROOT_PASSWORD in config file!" -ForegroundColor Red
    Write-Host "Config file should contain: DROPLET_IP=your_ip and ROOT_PASSWORD=your_password" -ForegroundColor Red
    exit 1
}

Write-Host "UK Energy Grid Tracker - Clean Deployment" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host "Droplet IP: $DropletIP" -ForegroundColor Yellow
Write-Host ""

# Step 1: Verify required files exist
Write-Host "Step 1: Verifying required files..." -ForegroundColor Yellow

$requiredFiles = @(
    "app",
    "docker-compose.prod.yml",
    "Dockerfile",
    "requirements.txt"
)

foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        Write-Host "[ERROR] Required file/directory '$file' not found!" -ForegroundColor Red
        Write-Host "Make sure you're running this script from the project root directory." -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Found $file" -ForegroundColor Green
}

# Step 2: Test SSH connection
Write-Host "`nStep 2: Testing SSH connection..." -ForegroundColor Yellow

$sshJob = Start-Job -ScriptBlock {
    param($ip)
    $output = ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@$ip "echo 'SSH connection successful'" 2>$null
    $exitCode = $LASTEXITCODE
    return @{Output = $output; ExitCode = $exitCode}
} -ArgumentList $DropletIP

if (Wait-Job $sshJob -Timeout 10) {
    $result = Receive-Job $sshJob
    Remove-Job $sshJob
    
    if ($result.ExitCode -eq 0) {
        Write-Host "[OK] SSH connection successful" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] SSH connection failed - check your SSH key setup" -ForegroundColor Red
        Write-Host "Make sure you have SSH keys set up for passwordless access." -ForegroundColor Red
        exit 1
    }
} else {
    Remove-Job $sshJob -Force
    Write-Host "[ERROR] SSH connection timed out after 10 seconds" -ForegroundColor Red
    exit 1
}

# Step 3: Stop and remove old containers
Write-Host "`nStep 3: Stopping old containers..." -ForegroundColor Yellow

try {
    echo $RootPassword | ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no root@$DropletIP "cd /opt/grid-tracker && docker-compose down --remove-orphans" 2>$null
    Write-Host "[OK] Old containers stopped" -ForegroundColor Green
} catch {
    Write-Host "[WARNING] Could not stop old containers: $($_.Exception.Message)" -ForegroundColor Yellow
}

# Step 4: Create directories on server
Write-Host "`nStep 4: Creating directories on server..." -ForegroundColor Yellow

$dirJob = Start-Job -ScriptBlock {
    param($ip)
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@$ip "mkdir -p /opt/grid-tracker/database /opt/grid-tracker/logs" 2>$null
    return $LASTEXITCODE
} -ArgumentList $DropletIP

if (Wait-Job $dirJob -Timeout 15) {
    $result = Receive-Job $dirJob
    Remove-Job $dirJob
    
    if ($result -eq 0) {
        Write-Host "[OK] Directories created" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Failed to create directories" -ForegroundColor Red
        exit 1
    }
} else {
    Remove-Job $dirJob -Force
    Write-Host "[ERROR] Directory creation timed out" -ForegroundColor Red
    exit 1
}

# Step 5: Copy files to server
Write-Host "`nStep 5: Copying files to server..." -ForegroundColor Yellow

# Copy app directory
Write-Host "Copying app directory..." -ForegroundColor Cyan
$scpJob1 = Start-Job -ScriptBlock {
    param($ip, $scriptDir)
    Set-Location $scriptDir
    $output = scp -o ConnectTimeout=5 -o StrictHostKeyChecking=no -r "app" "root@${ip}:/opt/grid-tracker/" 2>&1
    $exitCode = $LASTEXITCODE
    return @{Output = $output; ExitCode = $exitCode}
} -ArgumentList $DropletIP, $PWD

if (Wait-Job $scpJob1 -Timeout 60) {
    $result = Receive-Job $scpJob1
    Remove-Job $scpJob1
    
    if ($result.ExitCode -eq 0) {
        Write-Host "[OK] app directory copied" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Failed to copy app directory" -ForegroundColor Red
        Write-Host "Error output: $($result.Output)" -ForegroundColor Red
        exit 1
    }
} else {
    Remove-Job $scpJob1 -Force
    Write-Host "[ERROR] app directory copy timed out" -ForegroundColor Red
    exit 1
}

# Copy other files
$filesToCopy = @(
    "docker-compose.prod.yml",
    "Dockerfile",
    "requirements.txt",
    "clear_table_data.py",
    "interpolate_single_gaps.py",
    "normalize_database_timestamps.py",
    "migrate_deduplicate_and_unique.py",
    "migrate_add_timestamp_sql.py",
    "run_all_migrations.py",
    "migrate_add_total_column.py"
)
foreach ($file in $filesToCopy) {
    Write-Host "Copying $file..." -ForegroundColor Cyan
    $scpJob = Start-Job -ScriptBlock {
        param($ip, $scriptDir, $filename)
        Set-Location $scriptDir
        $output = scp -o ConnectTimeout=5 -o StrictHostKeyChecking=no $filename "root@${ip}:/opt/grid-tracker/" 2>&1
        $exitCode = $LASTEXITCODE
        return @{Output = $output; ExitCode = $exitCode}
    } -ArgumentList $DropletIP, $PWD, $file

    if (Wait-Job $scpJob -Timeout 30) {
        $result = Receive-Job $scpJob
        Remove-Job $scpJob
        
        if ($result.ExitCode -eq 0) {
            Write-Host "[OK] $file copied" -ForegroundColor Green
        } else {
            Write-Host "[ERROR] Failed to copy $file" -ForegroundColor Red
            Write-Host "Error output: $($result.Output)" -ForegroundColor Red
            exit 1
        }
    } else {
        Remove-Job $scpJob -Force
        Write-Host "[ERROR] $file copy timed out" -ForegroundColor Red
        exit 1
    }
}

# Step 6: Deploy on server
Write-Host "`nStep 6: Deploying on server..." -ForegroundColor Yellow
try {
    # Rename docker-compose file
    echo $RootPassword | ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no root@$DropletIP "cd /opt/grid-tracker && mv docker-compose.prod.yml docker-compose.yml" 2>$null
    
    # Build and start containers
    echo $RootPassword | ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no root@$DropletIP "cd /opt/grid-tracker && docker-compose up --build -d" 2>$null
    
    if ($LASTEXITCODE -ne 0) {
        throw "Deployment failed"
    }
    Write-Host "[OK] Deployment completed" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Deployment failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Step 7: Verify deployment
Write-Host "`nStep 7: Verifying deployment..." -ForegroundColor Yellow

# Wait for container to start
Start-Sleep -Seconds 10

# Check container status
try {
    $status = echo $RootPassword | ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@$DropletIP "cd /opt/grid-tracker && docker-compose ps" 2>$null
    Write-Host "Container status:" -ForegroundColor Cyan
    Write-Host $status
} catch {
    Write-Host "[WARNING] Could not get container status" -ForegroundColor Yellow
}

# Check recent logs
try {
    $logs = echo $RootPassword | ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@$DropletIP "cd /opt/grid-tracker && docker-compose logs --tail=20" 2>$null
    Write-Host "Recent logs:" -ForegroundColor Cyan
    Write-Host $logs
} catch {
    Write-Host "[WARNING] Could not get recent logs" -ForegroundColor Yellow
}

Write-Host "`nDeployment complete!" -ForegroundColor Green
Write-Host "To monitor logs: ssh root@$DropletIP 'cd /opt/grid-tracker && docker-compose logs -f'" -ForegroundColor White
Write-Host "To check status: ssh root@$DropletIP 'cd /opt/grid-tracker && docker-compose ps'" -ForegroundColor White 