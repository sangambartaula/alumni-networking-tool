import mysql.connector
import os
from dotenv import load_dotenv
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env from project root
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

def test_connection():
    """Test MySQL connection and print diagnostic information"""
    host = os.getenv("MYSQLHOST")
    user = os.getenv("MYSQLUSER")
    password = os.getenv("MYSQLPASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    port = int(os.getenv("MYSQLPORT", "3306"))

    logger.info(f"Testing connection to MySQL server at {host}:{port}")
    logger.info(f"Database: {database}")
    logger.info(f"User: {user}")

    try:
        # Try connection without database first
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            connect_timeout=5
        )
        logger.info("✅ Successfully connected to MySQL server!")
        
        # Test database access
        try:
            conn.cursor().execute(f"USE {database}")
            logger.info(f"✅ Successfully accessed database '{database}'")
            
            # Test table access
            try:
                cur = conn.cursor()
                cur.execute("SHOW TABLES")
                tables = [table[0] for table in cur.fetchall()]
                logger.info(f" Found tables: {', '.join(tables)}")
            except mysql.connector.Error as e:
                logger.error(f" Error listing tables: {e}")
                
        except mysql.connector.Error as e:
            logger.error(f"Error accessing database '{database}': {e}")
        
        conn.close()
        
    except mysql.connector.Error as err:
        logger.error(" Connection failed!")
        if err.errno == 2003:
            logger.error("   Cannot connect to MySQL server. Possible causes:")
            logger.error("   1. Server is not running")
            logger.error("   2. Security group doesn't allow your IP")
            logger.error("   3. Network/VPC/subnet configuration issue")
        elif err.errno == 1045:
            logger.error("   Access denied - incorrect username or password")
        else:
            logger.error(f"   Error {err.errno}: {err.msg}")


if __name__ == "__main__":
    test_connection()
