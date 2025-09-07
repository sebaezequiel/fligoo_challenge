import os
import sqlalchemy as sa

def get_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "fligoo")
    pwd  = os.getenv("DB_PASS", "fligoo")
    db   = os.getenv("DB_NAME", "testfligoo")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return sa.create_engine(url, pool_pre_ping=True, future=True)
