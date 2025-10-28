"""
Database configuration file using keyring for secure credential storage
"""

import os
import keyring

# This file provides a way to securely retrieve database credentials
# using the keyring library, which stores credentials in a secure manner.
# 1. Install the keyring library if you haven't already:
#    pip install keyring
# 2. Set your database password in the keyring using the following command in Python:
#    import keyring
#    keyring.set_password('YOUR CONFIGURED SERVICE NAME', 'YOUR USERNAME', 'YOUR PASSWORD')
#    Replace 'YOUR CONFIGURED SERVICE NAME', 'YOUR USERNAME', and 'YOUR PASSWORD' with your actual service name, username, and password.


def get_db_credentials():
    """Get database credentials from keyring"""
    # Service name for keyring - change this to something unique for your project
    service_name = "YOUR CONFIGURED SERVICE NAME"
    username = "INSERT USERNAME HERE"  # Replace with your actual username
    
    # Try to get password from keyring
    password = keyring.get_password(service_name, username)
    
    if password is None:
        raise ValueError(f"No password found in keyring for service '{service_name}' and user '{username}'. "
                        f"Please set it first using: keyring.set_password('{service_name}', '{username}', 'your_password')")
    
    return username, password

# Database configuration using keyring
def get_db_config():
    username, password = get_db_credentials()
    
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': username,
        'password': password,
        'port': int(os.getenv('DB_PORT', 3309)),
        'database': os.getenv('DB_NAME', 'nineamdwh'),
        'autocommit': True
    }

# For backward compatibility
DB_CONFIG = get_db_config()