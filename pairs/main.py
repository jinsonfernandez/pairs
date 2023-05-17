from configparser import ConfigParser
from src.database.connectors import DatabaseConnector
from src.etl.aggregation import AggregationETL

def main():
    # Read credentials from the config file
    config = ConfigParser()
    config.read("C:\\Users\\Jinson\\Desktop\\UpSkill\\pairs\\pairs\\config\\credentials.ini")  

    pg_host = config.get("PostgreSQL", "host")
    pg_port = config.getint("PostgreSQL", "port")
    pg_db = config.get("PostgreSQL", "database")
    pg_user = config.get("PostgreSQL", "user")
    pg_password = config.get("PostgreSQL", "password")

    mysql_host = config.get("MySQL", "host")
    mysql_port = config.getint("MySQL", "port")
    mysql_db = config.get("MySQL", "database")
    mysql_user = config.get("MySQL", "user")
    mysql_password = config.get("MySQL", "password")

    # Create PostgreSQL connector instance
    pg_connector = DatabaseConnector(
        pg_host, pg_port, pg_db, pg_user, pg_password,
        mysql_host, mysql_port, mysql_db, mysql_user, mysql_password
    )

    # Create MySQL connector instance
    mysql_connector = DatabaseConnector(
        pg_host, pg_port, pg_db, pg_user, pg_password,
        mysql_host, mysql_port, mysql_db, mysql_user, mysql_password
    )

    # Create AggregationETL instance
    etl = AggregationETL(pg_connector, mysql_connector)

    # Pull data from PostgreSQL
    data = etl.pull_data_from_postgres()

    # Calculate aggregations
    max_temperatures, data_points, total_distance = etl.calculate_aggregations(data)

    # Store aggregated data in MySQL
    etl.store_aggregated_data_to_mysql(max_temperatures, data_points, total_distance)


if __name__ == "__main__":
    main()
