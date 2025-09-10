import os
from dotenv import load_dotenv

# Load ENV
load_dotenv()

def get_postgres_jdbc():
    return {
        "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


