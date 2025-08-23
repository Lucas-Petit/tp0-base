#!/usr/bin/env python3
"""
Docker Compose generator script
Generates a docker-compose.yaml file with configurable number of clients
"""

import sys
import yaml

def generate_compose_config(num_clients):
    """Generate Docker Compose configuration with specified number of clients"""
    
    compose_config = {
        'name': 'tp0',
        'services': {
            'server': {
                'container_name': 'server',
                'image': 'server:latest',
                'entrypoint': 'python3 /main.py',
                'environment': [
                    'PYTHONUNBUFFERED=1'
                ],
                'networks': ['testing_net'],
                'volumes': [
                    './server/config.ini:/config.ini:ro'
                ],
            }
        },
        'networks': {
            'testing_net': {
                'ipam': {
                    'driver': 'default',
                    'config': [
                        {'subnet': '172.25.125.0/24'}
                    ]
                }
            }
        }
    }
    
    for i in range(1, num_clients + 1):
        client_name = f'client{i}'
        compose_config['services'][client_name] = {
            'container_name': client_name,
            'image': 'client:latest',
            'entrypoint': '/client',
            'environment': [
                f'CLI_ID={i}'
            ],
            'networks': ['testing_net'],
            'depends_on': ['server'],
            'volumes': [
                './client/config.yaml:/config.yaml:ro'
            ],
        }
    
    return compose_config

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 generar-compose.py <output_file> <number_of_clients>")
        sys.exit(1)
    
    output_file = sys.argv[1]
    try:
        num_clients = int(sys.argv[2])
        if num_clients < 0:
            raise ValueError("Number of clients must be positive")
    except ValueError as e:
        print(f"Error: Invalid number of clients. {e}")
        sys.exit(1)
    
    config = generate_compose_config(num_clients)
    
    try:
        with open(output_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2)
        print(f"Successfully generated {output_file} with {num_clients} clients")
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
