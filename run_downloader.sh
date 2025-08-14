#!/bin/bash
#
# BOP Málaga PDF Downloader - Hourly Execution Wrapper Script
#
# This script serves as the main entry point for the automated BOP Málaga
# PDF downloader system. It handles:
#
# - Virtual environment activation
# - Python script execution with proper error handling
# - Output capture and logging
# - Runtime failure recovery
# - System health checks
# - Lock file management to prevent concurrent executions
#
# Usage: ./run_downloader.sh [options]
# Options:
#   --config FILE    Use custom configuration file
#   --verbose        Enable verbose output
#   --dry-run        Run without downloading files
#   --force          Force execution even if lock file exists
#   --help           Show this help message
#
# Author: Automated Deployment System
# Date: 2025

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"
LOCK_FILE="${SCRIPT_DIR}/.downloader.lock"
LOG_FILE="${SCRIPT_DIR}/logs/wrapper.log"
PID_FILE="${SCRIPT_DIR}/.downloader.pid"

# Virtual environment and Python script paths
VENV_DIR="${SCRIPT_DIR}/venv"
PYTHON_SCRIPT="${SCRIPT_DIR}/bop_malaga_downloader.py"
PYTHON_BIN="${VENV_DIR}/bin/python"

# Default configuration
CONFIG_FILE=""
VERBOSE=false
DRY_RUN=false
FORCE_EXECUTION=false
MAX_EXECUTION_TIME=3600  # 1 hour timeout
HEALTH_CHECK_ENABLED=true

# Logging functions
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Ensure log directory exists
    mkdir -p "$(dirname "$LOG_FILE")"
    
    # Log to file
    echo "[$timestamp] [$level] [$SCRIPT_NAME] $message" >> "$LOG_FILE"
    
    # Log to console if verbose or error/warning
    if [[ "$VERBOSE" == "true" ]] || [[ "$level" == "ERROR" ]] || [[ "$level" == "WARN" ]]; then
        echo "[$timestamp] [$level] $message" >&2
    fi
}

log_info() {
    log_message "INFO" "$1"
}

log_warn() {
    log_message "WARN" "$1"
}

log_error() {
    log_message "ERROR" "$1"
}

log_debug() {
    if [[ "$VERBOSE" == "true" ]]; then
        log_message "DEBUG" "$1"
    fi
}

# Help function
show_help() {
    cat << EOF
BOP Málaga PDF Downloader - Hourly Execution Wrapper Script

Usage: $SCRIPT_NAME [options]

Options:
    --config FILE    Use custom configuration file
    --verbose        Enable verbose output and logging
    --dry-run        Run without actually downloading files
    --force          Force execution even if lock file exists
    --help           Show this help message and exit

Environment Variables:
    BOP_CONFIG_FILE     Path to configuration file
    BOP_MAX_EXEC_TIME   Maximum execution time in seconds (default: 3600)
    BOP_VERBOSE         Enable verbose logging (true/false)

Examples:
    $SCRIPT_NAME                           # Normal execution
    $SCRIPT_NAME --verbose                 # Verbose execution
    $SCRIPT_NAME --config /path/config.json  # Custom config
    $SCRIPT_NAME --dry-run --verbose      # Test run with verbose output

Exit Codes:
    0   Success
    1   General error
    2   Lock file exists (another instance running)
    3   Virtual environment not found
    4   Python script not found
    5   Execution timeout
    6   Configuration error

EOF
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --force)
                FORCE_EXECUTION=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Load environment variables
    if [[ -n "${BOP_CONFIG_FILE:-}" ]]; then
        CONFIG_FILE="${BOP_CONFIG_FILE}"
    fi
    
    if [[ -n "${BOP_MAX_EXEC_TIME:-}" ]]; then
        MAX_EXECUTION_TIME="${BOP_MAX_EXEC_TIME}"
    fi
    
    if [[ "${BOP_VERBOSE:-false}" == "true" ]]; then
        VERBOSE=true
    fi
}

# Check if another instance is running
check_lock_file() {
    if [[ -f "$LOCK_FILE" ]] && [[ "$FORCE_EXECUTION" != "true" ]]; then
        local lock_pid
        lock_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
        
        if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
            log_error "Another instance is already running (PID: $lock_pid)"
            log_error "Use --force to override, or wait for the current execution to complete"
            exit 2
        else
            log_warn "Stale lock file found, removing it"
            rm -f "$LOCK_FILE"
        fi
    fi
}

# Create lock file
create_lock_file() {
    echo $$ > "$LOCK_FILE"
    echo $$ > "$PID_FILE"
    log_debug "Created lock file with PID: $$"
}

# Remove lock file
remove_lock_file() {
    if [[ -f "$LOCK_FILE" ]]; then
        rm -f "$LOCK_FILE"
        log_debug "Removed lock file"
    fi
    
    if [[ -f "$PID_FILE" ]]; then
        rm -f "$PID_FILE"
        log_debug "Removed PID file"
    fi
}

# Cleanup function for trap
cleanup() {
    local exit_code=$?
    log_debug "Cleanup function called with exit code: $exit_code"
    remove_lock_file
    
    if [[ $exit_code -ne 0 ]]; then
        log_error "Script exited with error code: $exit_code"
    fi
    
    exit $exit_code
}

# Set up signal handlers
setup_signal_handlers() {
    trap cleanup EXIT
    trap 'log_warn "Received SIGINT, shutting down gracefully..."; exit 130' INT
    trap 'log_warn "Received SIGTERM, shutting down gracefully..."; exit 143' TERM
}

# Validate environment
validate_environment() {
    log_debug "Validating execution environment"
    
    # Check if virtual environment exists
    if [[ ! -d "$VENV_DIR" ]]; then
        log_error "Virtual environment not found: $VENV_DIR"
        log_error "Please run the setup script first to create the virtual environment"
        exit 3
    fi
    
    # Check if Python binary exists in virtual environment
    if [[ ! -f "$PYTHON_BIN" ]]; then
        log_error "Python binary not found: $PYTHON_BIN"
        log_error "Virtual environment may be corrupted"
        exit 3
    fi
    
    # Check if main Python script exists
    if [[ ! -f "$PYTHON_SCRIPT" ]]; then
        log_error "Python script not found: $PYTHON_SCRIPT"
        exit 4
    fi
    
    # Check if Python script is executable
    if [[ ! -x "$PYTHON_SCRIPT" ]]; then
        log_debug "Making Python script executable"
        chmod +x "$PYTHON_SCRIPT"
    fi
    
    # Validate configuration file if specified
    if [[ -n "$CONFIG_FILE" ]]; then
        if [[ ! -f "$CONFIG_FILE" ]]; then
            log_error "Configuration file not found: $CONFIG_FILE"
            exit 6
        fi
        
        # Test configuration file validity
        if ! "$PYTHON_BIN" -c "import json; json.load(open('$CONFIG_FILE'))" 2>/dev/null; then
            log_error "Invalid JSON in configuration file: $CONFIG_FILE"
            exit 6
        fi
    fi
    
    log_debug "Environment validation completed successfully"
}

# Perform system health checks
perform_health_checks() {
    if [[ "$HEALTH_CHECK_ENABLED" != "true" ]]; then
        return 0
    fi
    
    log_debug "Performing system health checks"
    
    # Check available disk space
    local available_space
    available_space=$(df "$SCRIPT_DIR" | awk 'NR==2 {print $4}')
    local min_space=1048576  # 1GB in KB
    
    if [[ "$available_space" -lt "$min_space" ]]; then
        log_warn "Low disk space: ${available_space}KB available (minimum: ${min_space}KB)"
    fi
    
    # Check if we can write to logs directory
    local test_log="${SCRIPT_DIR}/logs/.write_test"
    if ! touch "$test_log" 2>/dev/null; then
        log_error "Cannot write to logs directory: ${SCRIPT_DIR}/logs"
        exit 1
    fi
    rm -f "$test_log"
    
    # Check network connectivity (basic test)
    if ! ping -c 1 -W 5 8.8.8.8 >/dev/null 2>&1; then
        log_warn "Network connectivity test failed - downloads may not work"
    fi
    
    log_debug "Health checks completed"
}

# Execute the Python downloader with timeout
execute_downloader() {
    log_info "Starting BOP Málaga PDF downloader execution"
    
    # Build command arguments
    local cmd_args=()
    
    if [[ -n "$CONFIG_FILE" ]]; then
        cmd_args+=("--config" "$CONFIG_FILE")
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        cmd_args+=("--verbose")
    fi
    
    # Create temporary files for output capture
    local stdout_file=$(mktemp)
    local stderr_file=$(mktemp)
    
    # Ensure cleanup of temp files
    trap 'rm -f "$stdout_file" "$stderr_file"' RETURN
    
    log_debug "Executing: $PYTHON_BIN $PYTHON_SCRIPT ${cmd_args[*]}"
    
    # Execute with timeout
    local start_time=$(date +%s)
    local exit_code=0
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would execute Python downloader with args: ${cmd_args[*]}"
        echo "DRY RUN - No actual execution performed" > "$stdout_file"
        exit_code=0
    else
        # Use timeout command if available, otherwise use background process with kill
        if command -v timeout >/dev/null 2>&1; then
            timeout "$MAX_EXECUTION_TIME" "$PYTHON_BIN" "$PYTHON_SCRIPT" "${cmd_args[@]}" \
                > "$stdout_file" 2> "$stderr_file" || exit_code=$?
        else
            # Fallback: background process with manual timeout
            "$PYTHON_BIN" "$PYTHON_SCRIPT" "${cmd_args[@]}" \
                > "$stdout_file" 2> "$stderr_file" &
            local python_pid=$!
            
            # Wait for completion or timeout
            local elapsed=0
            while kill -0 "$python_pid" 2>/dev/null && [[ $elapsed -lt $MAX_EXECUTION_TIME ]]; do
                sleep 5
                elapsed=$((elapsed + 5))
            done
            
            # Check if process is still running (timeout)
            if kill -0 "$python_pid" 2>/dev/null; then
                log_error "Execution timeout after ${MAX_EXECUTION_TIME} seconds"
                kill -TERM "$python_pid" 2>/dev/null || true
                sleep 5
                kill -KILL "$python_pid" 2>/dev/null || true
                exit_code=5
            else
                wait "$python_pid" || exit_code=$?
            fi
        fi
    fi
    
    local end_time=$(date +%s)
    local execution_time=$((end_time - start_time))
    
    # Process output
    local stdout_content=""
    local stderr_content=""
    
    if [[ -f "$stdout_file" ]]; then
        stdout_content=$(cat "$stdout_file")
    fi
    
    if [[ -f "$stderr_file" ]]; then
        stderr_content=$(cat "$stderr_file")
    fi
    
    # Log execution results
    log_info "Python downloader execution completed in ${execution_time} seconds with exit code: $exit_code"
    
    if [[ -n "$stdout_content" ]]; then
        log_debug "STDOUT output:"
        while IFS= read -r line; do
            log_debug "  $line"
        done <<< "$stdout_content"
    fi
    
    if [[ -n "$stderr_content" ]]; then
        if [[ $exit_code -eq 0 ]]; then
            log_debug "STDERR output:"
            while IFS= read -r line; do
                log_debug "  $line"
            done <<< "$stderr_content"
        else
            log_error "STDERR output:"
            while IFS= read -r line; do
                log_error "  $line"
            done <<< "$stderr_content"
        fi
    fi
    
    # Handle different exit codes
    case $exit_code in
        0)
            log_info "Downloader execution completed successfully"
            ;;
        1)
            log_error "Downloader execution failed with general error"
            ;;
        5|124)  # timeout exit codes
            log_error "Downloader execution timed out after ${MAX_EXECUTION_TIME} seconds"
            exit 5
            ;;
        130)
            log_warn "Downloader execution interrupted by user (SIGINT)"
            ;;
        143)
            log_warn "Downloader execution terminated (SIGTERM)"
            ;;
        *)
            log_error "Downloader execution failed with unexpected exit code: $exit_code"
            ;;
    esac
    
    return $exit_code
}

# Generate execution summary
generate_summary() {
    local exit_code=$1
    local start_time="$2"
    local end_time=$(date +%s)
    local total_time=$((end_time - start_time))
    
    log_info "=== Execution Summary ==="
    log_info "Script: $SCRIPT_NAME"
    log_info "Start time: $(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S')"
    log_info "End time: $(date -d "@$end_time" '+%Y-%m-%d %H:%M:%S')"
    log_info "Total execution time: ${total_time} seconds"
    log_info "Exit code: $exit_code"
    log_info "PID: $$"
    
    if [[ -n "$CONFIG_FILE" ]]; then
        log_info "Configuration file: $CONFIG_FILE"
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "Mode: DRY RUN"
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        log_info "Verbose logging: enabled"
    fi
    
    log_info "========================="
}

# Main execution function
main() {
    local script_start_time=$(date +%s)
    
    # Parse command line arguments
    parse_arguments "$@"
    
    # Set up signal handlers
    setup_signal_handlers
    
    log_info "Starting BOP Málaga PDF Downloader wrapper script"
    log_debug "Script directory: $SCRIPT_DIR"
    log_debug "Lock file: $LOCK_FILE"
    log_debug "Log file: $LOG_FILE"
    
    # Check for concurrent execution
    check_lock_file
    
    # Create lock file
    create_lock_file
    
    # Validate environment
    validate_environment
    
    # Perform health checks
    perform_health_checks
    
    # Execute the downloader
    local downloader_exit_code=0
    execute_downloader || downloader_exit_code=$?
    
    # Generate summary
    generate_summary $downloader_exit_code "$script_start_time"
    
    # Exit with the same code as the downloader
    exit $downloader_exit_code
}

# Execute main function with all arguments
main "$@"
