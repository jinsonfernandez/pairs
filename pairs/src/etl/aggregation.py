import math
from src.database import DatabaseConnector

class AggregationETL:
    def __init__(self, pg_connector, mysql_connector):
        self.database_connector = DatabaseConnector(pg_connector, mysql_connector)

    def pull_data_from_postgres(self):
        try:
            pg_conn = self.database_connector.get_postgres_connection()
            pg_cursor = pg_conn.cursor()

            pg_cursor.execute("SELECT device_id, temperature, location, time FROM Device")
            data = pg_cursor.fetchall()

            return data
        except Exception as e:
            print(f"Error pulling data from PostgreSQL: {str(e)}")
            return []

    def calculate_aggregations(self, data):
        max_temperatures = {}
        data_points = {}
        total_distance = {}

        for row in data:
            device_id, temperature, location, time = row
            # Calculate maximum temperature per device per hour
            if device_id not in max_temperatures or temperature > max_temperatures[device_id]:
                max_temperatures[device_id] = temperature
            
            # Calculate data points aggregated per device per hour
            hour = time.hour
            if device_id not in data_points:
                data_points[device_id] = {}
            if hour not in data_points[device_id]:
                data_points[device_id][hour] = 0
            data_points[device_id][hour] += 1
            
            # Calculate total distance of device movement per device per hour
            if device_id not in total_distance:
                total_distance[device_id] = 0
            if len(location) == 2:
                lat1, lon1 = location["latitude"], location["longitude"]
                lat2, lon2 = location["latitude"], location["longitude"]
                distance = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)) * 6371
                total_distance[device_id] += distance

        return max_temperatures, data_points, total_distance

    def store_aggregated_data_to_mysql(self, max_temperatures, data_points, total_distance):
        try:
            mysql_conn = self.database_connector.get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()

            # Create a new table for storing aggregated data if it doesn't exist
            mysql_cursor.execute("""
                CREATE TABLE IF NOT EXISTS AggregatedData (
                    device_id VARCHAR(36),
                    hour INT,
                    max_temperature INT,
                    data_points INT,
                    total_distance FLOAT,
                    PRIMARY KEY (device_id, hour)
                )
            """)

            # Insert the aggregated data into the MySQL table
            for device_id, temps in max_temperatures.items():
                for hour, temperature in temps.items():
                    data_point_count = data_points.get(device_id, {}).get(hour, 0)
                    distance = total_distance.get(device_id, 0)
                    mysql_cursor.execute("""
                        INSERT INTO AggregatedData (device_id, hour, max_temperature, data_points, total_distance)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (device_id, hour, temperature, data_point_count, distance))
            
            mysql_conn.commit()
            print("Aggregated data stored in MySQL successfully!")
        except Exception as e:
            print(f"Error storing aggregated data in MySQL: {str(e)}")

