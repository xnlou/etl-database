import os

# Database connection parameters
DB_PARAMS = {
    "dbname": "feeds",
    "user": "yostfundsadmin",
    "password": os.getenv("DB_PASSWORD", "etlserver2025!"),
    "host": "localhost",
    "port": "5432"
}

# SQLAlchemy database URL (for scripts using SQLAlchemy)
SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}"
    f"@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
)