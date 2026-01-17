import os

# Use environment variable for production, fallback to local config for development
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    # Production: Parse DATABASE_URL (Render/Heroku format)
    # Format: postgresql://user:password@host:port/dbname
    import urllib.parse as urlparse
    url = urlparse.urlparse(DATABASE_URL)
    DB_CONFIG = {
        "dbname": url.path[1:],
        "user": url.username,
        "password": url.password,
        "host": url.hostname,
        "port": url.port
    }
else:
    # Development: Local PostgreSQL
    DB_CONFIG = {
        "dbname": "listings_db",
        "user": "postgres",
        "password": "#Aryank5651!",
        "host": "localhost",
        "port": 5432
    }
