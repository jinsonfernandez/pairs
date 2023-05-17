import psycopg2
import pymysql

class DatabaseConnector:
    def __init__(self, pg_host, pg_port, pg_db, pg_user, pg_password, mysql_host, mysql_port, mysql_db, mysql_user, mysql_password):
        self.pg_conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_db, user=pg_user, password=pg_password)
        self.mysql_conn = pymysql.connect(host=mysql_host, port=mysql_port, database=mysql_db, user=mysql_user, password=mysql_password)
    
    def get_postgres_connection(self):
        return self.pg_conn
    
    def get_mysql_connection(self):
        return self.mysql_conn
