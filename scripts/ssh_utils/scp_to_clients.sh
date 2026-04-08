#!/bin/bash

# Usage check: exactly two arguments (source and target)
if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_path> <target_path>"
    exit 1
fi

SOURCE="$1"
TARGET="$2"
CLIENT_LIST="/etc/config/distributed_computing/client_list"

# Check if client list file exists and is readable
if [ ! -f "$CLIENT_LIST" ]; then
    echo "Error: Client list file $CLIENT_LIST not found."
    exit 1
fi

# Read clients from the file (one per line, ignore empty lines)
while IFS= read -r client || [ -n "$client" ]; do
    # Skip empty lines or lines that are only whitespace
    if [[ -z "${client// /}" ]]; then
        continue
    fi

    echo "Copying to $client ..."
    scp -r "$SOURCE" "HDLNoCGen@$client:\"$TARGET\""
    if [ $? -ne 0 ]; then
        echo "Warning: scp to $client failed." >&2
    fi
done < "$CLIENT_LIST"

