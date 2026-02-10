import mysql.connector
import os
import time
from dotenv import load_dotenv
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load .env from project root
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

def test_connection():
    """Test MySQL connection, latency, and basic query functionality."""
    host = os.getenv("MYSQLHOST")
    user = os.getenv("MYSQLUSER")
    password = os.getenv("MYSQLPASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    port = int(os.getenv("MYSQLPORT", "3306"))

    logger.info(f"Testing connection to MySQL server at {host}:{port}")
    logger.info(f"Database: {database} | User: {user}")

    start_time = time.time()

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        connect_timeout=5
    )

    latency = round((time.time() - start_time) * 1000, 2)
    logger.info(f"Connected successfully (latency: {latency} ms)")

    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [t[0] for t in cur.fetchall()]
        assert tables is not None  # basic sanity check

    with conn.cursor() as cur:
        cur.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS test_table "
            "(id INT PRIMARY KEY AUTO_INCREMENT, msg VARCHAR(255))"
        )
        cur.execute("INSERT INTO test_table (msg) VALUES ('DB connection test successful')")
        conn.commit()
        cur.execute("SELECT msg FROM test_table LIMIT 1")
        result = cur.fetchone()

        assert result is not None
        assert result[0] == "DB connection test successful"

    conn.close()
    logger.info("Connection closed cleanly.")


if __name__ == "__main__":
    success = test_connection()
    if success:
        logger.info(" Database connectivity test PASSED")
    else:
        logger.error(" Database connectivity test FAILED")
