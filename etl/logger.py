import logging, os
from datetime import datetime

def setup_logger(name = "etl_pipeline"):
    """Function to setup a logger for ETL pipeline."""
    log_dir = os.getenv("LOG_DIR", "/opt/airflow/logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ],
    )
    
    return logging.getLogger(name)
    