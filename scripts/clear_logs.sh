#!/bin/bash

# Clear log files in the logs directory
LOG_DIR="./logs"
INTERNAL_METRICS_DIR="./internal_metrics"
DATABASE_DIR="./job"

echo "Clearing log files in $LOG_DIR"
rm -rf $LOG_DIR/*
echo "Log files cleared."
echo "Clearing internal metrics in $INTERNAL_METRICS_DIR"
rm -rf $INTERNAL_METRICS_DIR/*
echo "Internal metrics cleared."
echo "Clearing database files in $DATABASE_DIR"
rm -rf $DATABASE_DIR/*
echo "Database files cleared."