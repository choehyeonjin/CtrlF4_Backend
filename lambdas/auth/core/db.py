import os
import psycopg2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lambda 실행 컨텍스트 외부에서 연결 객체를 초기화하여 재사용 가능하도록 함
db_connection = None

def get_db_connection():
  global db_connection

  if db_connection and db_connection.closed == 0:
    logger.info("Reusing existing database connection.")
    return db_connection
  
  try:
    logger.info("Creating a new database connection.")
    db_connection = psycopg2.connect(
      host=os.environ['DB_HOST'],
      port=os.environ['DB_PORT'],
      dbname=os.environ['DB_NAME'],
      user=os.environ['DB_USER'],
      password=os.environ['DB_PASSWORD']
    )
    return db_connection
  except psycopg2.Error as e:
    logger.error(f"Error: Could not connect to PostgreSQL instance. {e}")
    raise e