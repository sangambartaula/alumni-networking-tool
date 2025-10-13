import mysql.connector
import os
from dotenv import load_dotenv

# To specify the path to .env file
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

MYSQLHOST = os.getenv("MYSQLHOST")
MYSQLUSER = os.getenv("MYSQLUSER")
MYSQLPASSWORD = os.getenv("MYSQLPASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQLPORT = int(os.getenv("MYSQLPORT"))

try:
    conn = mysql.connector.connect(
        host=MYSQLHOST,
        user=MYSQLUSER,
        password=MYSQLPASSWORD,
        port=MYSQLPORT
    )

    cursor = conn.cursor()
    cursor.execute("SELECT NOW();")
    print("Connected to database. Current DB time:", cursor.fetchone())

    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    print("Error:", err)
