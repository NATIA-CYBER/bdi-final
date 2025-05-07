```mermaid
graph TB
    subgraph Data Sources
        OS[OpenSky Network]
        FC[Fuel Consumption Rates]
        AD[Aircraft Database]
    end

    subgraph AWS
        S3[S3 Data Lake]
        RDS[(PostgreSQL RDS)]
        subgraph S3 Layers
            Raw[Raw Layer]
            Prepared[Prepared Layer]
        end
    end

    subgraph Airflow Server
        AF[Airflow]
        DAG1[Aircraft DB DAG]
        DAG2[Fuel Consumption DAG]
        DAG3[Readsb History DAG]
    end

    subgraph API Server
        FA[FastAPI Service]
        E1[/aircrafts Endpoint]
        E2[/aircraft/{icao} Endpoint]
        E3[/aircraft/{icao}/co2 Endpoint]
    end

    OS --> DAG3
    FC --> DAG2
    AD --> DAG1

    DAG1 & DAG2 & DAG3 --> Raw
    Raw --> Prepared
    Prepared --> RDS

    RDS --> FA
    FA --> E1 & E2 & E3

    classDef aws fill:#FF9900,stroke:#232F3E,color:white;
    classDef airflow fill:#017CEE,stroke:#232F3E,color:white;
    classDef api fill:#009688,stroke:#232F3E,color:white;
    classDef source fill:#607D8B,stroke:#232F3E,color:white;

    class S3,RDS aws;
    class AF,DAG1,DAG2,DAG3 airflow;
    class FA,E1,E2,E3 api;
    class OS,FC,AD source;
```

# Architecture Overview

This diagram shows the main components of our aircraft data processing system:

1. **Data Sources**:
   - OpenSky Network: Aircraft tracking data
   - Fuel Consumption Rates: Aircraft type fuel consumption
   - Aircraft Database: Aircraft metadata

2. **AWS Infrastructure**:
   - S3 Data Lake with Raw and Prepared layers
   - PostgreSQL RDS for processed data storage

3. **Airflow Server**:
   - Three DAGs for data processing:
     - Aircraft Database DAG
     - Fuel Consumption DAG
     - Readsb History DAG (limited to 100 files/day)
   - Each DAG follows ETL pattern: Extract → Raw → Prepared → Load

4. **API Server**:
   - FastAPI service with three endpoints:
     - `/aircrafts`: List all aircraft
     - `/aircraft/{icao}`: Get specific aircraft
     - `/aircraft/{icao}/co2`: Calculate CO2 emissions

## Data Flow

1. DAGs fetch data from sources and store in S3 Raw layer
2. Data is processed and moved to Prepared layer
3. Processed data is loaded into PostgreSQL RDS
4. FastAPI endpoints serve data from PostgreSQL

## Key Features

- Idempotent data processing
- Proper data lake layering
- RESTful API with Pydantic models
- Scalable architecture with separate concerns
