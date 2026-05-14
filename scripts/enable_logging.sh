#!/bin/bash

# Define the path to the Postgres 12 config
# Based on your psql output, this is the standard Ubuntu path
PG_CONF="/etc/postgresql/12/main/postgresql.conf"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
  echo "Please run as root (use sudo)"
  exit
fi

echo "--- Backing up config to ${PG_CONF}.bak ---"
cp "$PG_CONF" "${PG_CONF}.bak"

echo "--- Updating PostgreSQL Logging Settings ---"

# 1. Enable logging_collector
# This regex handles both commented (#) and uncommented lines
sed -i "s/^#\?logging_collector.*/logging_collector = on/" "$PG_CONF"

# 4. Ensure auto_explain is in shared_preload_libraries if not already
# (Crucial for your query plan extraction)
if ! grep -q "auto_explain" "$PG_CONF"; then
    echo "--- Adding auto_explain to shared_preload_libraries ---"
    sed -i "s/^#\?shared_preload_libraries.*/shared_preload_libraries = 'auto_explain'/" "$PG_CONF"
fi

echo "--- Restarting PostgreSQL to apply changes ---"
systemctl restart postgresql

echo "--- Verification ---"
sudo -u postgres psql -c "SHOW logging_collector;"
sudo -u postgres psql -c "SELECT pg_current_logfile();"