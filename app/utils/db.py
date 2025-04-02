import re
from urllib.parse import quote_plus

def convert_libpq_to_sqlalchemy(libpq_conn_string):
    """
    Convert a libpq connection string (dbname=... format) to a SQLAlchemy connection URL.
    
    Args:
        libpq_conn_string (str): A connection string in libpq format 
                                 (e.g., "dbname=mydb user=user password=pwd host=localhost")
    
    Returns:
        str: A SQLAlchemy connection URL (postgresql://user:pwd@host:port/dbname)
    """
    # Initialize default values
    params = {
        'host': 'localhost',
        'port': '5432',
        'dbname': '',
        'user': '',
        'password': ''
    }
    
    # Extract key-value pairs from the libpq string
    pattern = r'(\w+)=([^\'"\s]+|\'[^\']*\'|"[^"]*")'
    matches = re.findall(pattern, libpq_conn_string)
    
    for key, value in matches:
        # Remove quotes if present
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        params[key] = value
    
    # For Azure, use the SSL mode
    ssl_mode = "require"
    
    # Construct the SQLAlchemy connection URL
    # URL encode the password to handle special characters
    password = quote_plus(params['password']) if params['password'] else ''
    
    # Create the connection URL
    sqlalchemy_url = f"postgresql://{params['user']}:{password}@{params['host']}:{params['port']}/{params['dbname']}?sslmode={ssl_mode}"
    
    return sqlalchemy_url
