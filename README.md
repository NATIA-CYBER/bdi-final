# Aircraft Data Pipeline and API

This project implements a data pipeline for processing aircraft tracking data and providing CO2 emissions calculations through a REST API.

## Architecture

The system consists of three main components:

1. **Airflow DAGs** for data processing:
   - `aircraft_db_dag.py`: Downloads and processes aircraft database
   - `fuel_consumption_dag.py`: Processes fuel consumption rates
   - `readsb_hist_dag.py`: Processes READSB tracking data

2. **FastAPI Application** for data access:
   - `/api/s8/aircrafts`: List all aircraft
   - `/aircraft/{icao}`: Get specific aircraft details
   - `/aircraft/{icao}/co2`: Calculate CO2 emissions

3. **AWS Infrastructure**:
   - S3 bucket for data lake (raw and prepared data)
   - RDS PostgreSQL for processed data
   - IAM roles for secure access

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- AWS account with appropriate permissions
- Terraform for infrastructure deployment

### Local Development

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/aircraft-data-pipeline.git
   cd aircraft-data-pipeline
   ```

2. Set up Python environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```bash
   cp bdi_api/.env.template bdi_api/.env
   # Edit .env with your credentials
   ```

4. Start Airflow:
   ```bash
   cd airflow
   docker-compose up -d
   ```

5. Start FastAPI:
   ```bash
   uvicorn bdi_api.app:app --reload
   ```

### Infrastructure Deployment

1. Initialize Terraform:
   ```bash
   cd infrastructure
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your values
   terraform init
   terraform plan
   terraform apply
   ```

## API Documentation

### Endpoints

1. `GET /api/s8/aircrafts`
   - Returns a list of all aircraft in the database
   - Response: Array of Aircraft objects

2. `GET /aircraft/{icao}`
   - Returns details for a specific aircraft
   - Parameters: `icao` (string)
   - Response: Aircraft object

3. `GET /aircraft/{icao}/co2`
   - Calculates CO2 emissions for an aircraft
   - Parameters: `icao` (string)
   - Response: CO2Emission object

## DAG Documentation

### Aircraft Database DAG
- Schedule: @daily
- Downloads aircraft database from OpenSky Network
- Processes and loads data into PostgreSQL

### Fuel Consumption DAG
- Schedule: @daily
- Processes aircraft fuel consumption rates
- Updates rates in the database

### READSB History DAG
- Schedule: @daily
- Limited to 100 files per execution
- Processes aircraft tracking data

## Testing

Run tests with:
```bash
pytest tests/ -v
```

Test results are documented in `api_test_results.md`.

## License

MIT