#!/bin/bash
# Description: Daily PostgreSQL backup script for all databases (including 'feeds' and schemas like 'dba').
set -e  # Exit on error

# Environment variables (set in ~/.bashrc or export for security)
DB_USER="yostfundsadmin"
DB_HOST="localhost"
DB_PASSWORD="etlserver2025!"  # Use env vars/secrets in production

# Directories (align with ETL project)
ROOT_DIR="$HOME/client_etl_workflow"
BACKUP_DIR="$ROOT_DIR/backups"
LOG_FILE="$ROOT_DIR/logs/pg_backup_all.log"
RETENTION_DAYS=3  # Updated to retain for 3 days

# Ensure directories exist with proper permissions
mkdir -p "$BACKUP_DIR" "$ROOT_DIR/logs"
chmod 770 "$BACKUP_DIR" "$ROOT_DIR/logs"

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Backup function (dumps all databases)
backup_all() {
    TIMESTAMP=$(date '+%Y%m%dT%H%M%S')
    BACKUP_FILE="$BACKUP_DIR/all_databases_${TIMESTAMP}.sql.gz"  # Compressed for space

    log "Starting full backup to $BACKUP_FILE"

    # Export password for pg_dumpall
    export PGPASSWORD="$DB_PASSWORD"

    pg_dumpall -h "$DB_HOST" -U "$DB_USER" | gzip > "$BACKUP_FILE"

    chmod 600 "$BACKUP_FILE"  # Restrict permissions
    log "Backup completed: $BACKUP_FILE"
}

# Rotation function
rotate_backups() {
    log "Rotating backups older than $RETENTION_DAYS days"
    find "$BACKUP_DIR" -type f -name "all_databases_*.sql.gz" -mtime +$RETENTION_DAYS -exec rm {} \;
    log "Rotation completed"
}

# Main execution
trap 'log "Error occurred during backup"; mail -s "Full Backup Failure" client@example.com < "$LOG_FILE"' ERR

backup_all
rotate_backups

log "Full backup job completed successfully"