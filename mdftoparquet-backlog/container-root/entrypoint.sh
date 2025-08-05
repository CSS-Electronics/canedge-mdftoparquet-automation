#!/bin/bash
set -e

# Check if CLOUD environment variable is set
if [ -z "$CLOUD" ]; then
  echo "ERROR: CLOUD environment variable is not set. Must be 'Azure', 'Amazon', or 'Google'."
  exit 1
fi

# Install requirements based on CLOUD environment variable
if [ "$CLOUD" = "Azure" ]; then
  echo "Installing Azure requirements..."
  pip install --no-cache-dir -r requirements_azure.txt
elif [ "$CLOUD" = "Amazon" ]; then
  echo "Installing Amazon requirements..."
  pip install --no-cache-dir -r requirements_amazon.txt
elif [ "$CLOUD" = "Google" ]; then
  echo "Installing Google requirements..."
  pip install --no-cache-dir -r requirements_google.txt
else
  echo "ERROR: Unsupported cloud provider: $CLOUD. Supported options: Azure, Amazon, Google"
  exit 1
fi

# Execute the original command
exec python process_backlog_container.py "$@"
