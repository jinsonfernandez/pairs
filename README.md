# Pairs

Pairs is a project that implements an ETL (Extract, Transform, Load) pipeline to aggregate data from a PostgreSQL database and store it in a MySQL database. The pipeline calculates various data aggregations such as maximum temperatures, data point counts, and total distance for each device per hour.

## Project Structure

The project follows the following structure:

pairs/
├── config/
│ └── credentials.ini
├── src/
│ ├── database/
│ │ └── connectors.py
│ └── etl/
│ └── aggregation.py
├── main.py
└── requirements.txt


- `config/`: Contains the `credentials.ini` file for configuring database credentials.
- `src/`: Contains the source code of the project.
  - `database/`: Provides the database connector class in `connectors.py`.
  - `etl/`: Contains the `aggregation.py` file with the ETL pipeline code.
- `main.py`: Entry point of the project.
- `requirements.txt`: Lists the project dependencies.

## Code Details

```shell
git clone https://github.com/jinsonfernandez/pairs.git

pip install -r requirements.txt

python main.py

