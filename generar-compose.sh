#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <output_file> <number_of_clients>"
    echo "Example: $0 docker-compose-dev.yaml 5"
    exit 1
fi

echo "Output file name: $1"
echo "Number of clients: $2"
python3 generar-compose.py "$1" "$2"
