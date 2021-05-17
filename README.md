# Packet Sniffer

### Project prerequisites
- Python 3.8.7
- Poetry package manager

### Usage

1.  `poetry install`
2. `poetry shell`
3. `sudo $(which python) ./packet-sniffer`

### Troubleshooting
Possible error with `psycopg2`, potential solution:  
`sudo apt install libpq-dev python3-dev`

# Data Exporter

### Usage
`sudo $(which python) ./data-exporter`

#  PostgreSQL + Grafana

### Prerequisites

Configure your local setup in `.env` file in project root, using `.env.example` as a template for your file

### Startup
1. `docker-compose up --env-file .env`
2. Go to  `localhost:3000` and create admin
3. Add PostgreSQL data source
4. Create a dashboard using `grafana_dashboard.json`

### Running tests
run `python -m unittest` in the main directory

### Troubleshooting
Possible error with adding PostgreSQL datasource, potential solution:  
Replace localhost with `host.docker.internal` or provide IP of PostgreSQL container