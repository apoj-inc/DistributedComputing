#!/bin/bash

# Usage check: exactly one arguments (cmd)
if [ $# -ne 1 ]; then
    echo "Usage: $0 <cmd>"
    exit 1
fi

CMD="$1"
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

    echo "Running on $client ..."
    ssh HDLNoCGen@$client "$CMD"&
    if [ $? -ne 0 ]; then
        echo "Warning: ssh on $client failed." >&2
    fi
done < "$CLIENT_LIST"

