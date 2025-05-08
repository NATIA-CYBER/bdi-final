import os
import re
from datetime import datetime
from typing import List, Optional

import boto3
import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field, validator

# Load environment variables
load_dotenv()

router = APIRouter()

# Custom Exceptions
class DatabaseConnectionError(Exception):
    pass

class DataValidationError(Exception):
    pass

# Pydantic Models
class Aircraft(BaseModel):
    icao: str
    registration: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    type_code: Optional[str] = None
    last_updated: datetime
    source: Optional[str] = None

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None
        }

    @validator('icao')
    def validate_icao(cls, v):
        if not v:
            raise ValueError('ICAO address is required')
        if not re.match(r'^[A-F0-9]{6}$', v.upper()):
            raise ValueError('ICAO must be 6 hex characters')
        return v.upper()

    @validator('registration')
    def validate_registration(cls, v):
        if v and not re.match(r'^[A-Z0-9-]+$', v):
            raise ValueError(
                'Registration must contain only uppercase letters, numbers, and hyphens'
            )
        return v

class FuelConsumption(BaseModel):
    aircraft_type: str = Field(..., description="Aircraft type")
    fuel_consumption_rate: float = Field(
        ..., description="Fuel consumption rate in kg/hour"
    )

class CO2Emission(BaseModel):
    total_co2: float = Field(..., description="Total CO2 emissions in kg")
    timestamp: datetime = Field(..., description="Timestamp of the calculation")

# Database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'airflow_2061a7-postgres-1'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"🔥 Database connection error: {str(e)}")
        raise DatabaseConnectionError(f"Database connection failed: {str(e)}")

# S3 client initialization function
def get_s3_client():
    try:
        region = os.getenv('AWS_REGION')
        if not region:
            region = 'us-east-1'  # Default region
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=region
        )
    except Exception as e:
        print(f"Warning: S3 client initialization failed: {e}")
        return None

@router.get("/aircrafts", response_model=List[Aircraft])
async def get_aircrafts(skip: int = 0, limit: int = 100):
    """Get all aircraft information from the database.
    Returns a list of aircraft with their details.
    """
    try:
        print("Starting get_aircrafts()")
        conn = get_db_connection()
        cur = conn.cursor()
        print("Got database connection")
        print("Database connection info:", conn.get_dsn_parameters())

        query = """
            WITH latest_records AS (
                SELECT DISTINCT ON (icao) *
                FROM aircraft
                ORDER BY icao, last_updated DESC
            )
            SELECT icao, registration, manufacturer, model, type_code, last_updated, source
            FROM latest_records
            OFFSET %s LIMIT %s;
        """
        cur.execute(query, (skip, min(limit, 100)))
        
        results = cur.fetchall()
        print(f"Query results: {results}")
        
        aircraft_list = []
        for r in results:
            try:
                print(f"Processing row: {r}")
                aircraft = Aircraft(**r)
                aircraft_list.append(aircraft)
            except Exception as e:
                print(f"Skipping invalid record: {e}")

        response_data = []
        for aircraft in aircraft_list:
            try:
                data = aircraft.dict()
                print(f"Aircraft data: {data}")
                response_data.append(data)
            except Exception as e:
                print(f"Error converting aircraft to dict: {e}")

        print(f"Final response data: {response_data}")
        return JSONResponse(content=response_data)

    except Exception as e:
        import traceback
        print("🔥 Exception in get_aircrafts:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to retrieve aircraft data: {str(e)}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@router.get("/aircraft/{icao}", response_model=Aircraft)
async def get_aircraft(icao: str):
    """
    Get detailed information about a specific aircraft by its ICAO address.
    """
    # Validate ICAO format
    if not re.match(r'^[A-F0-9]{6}$', icao.upper()):
        raise HTTPException(
            status_code=400,
            detail="Invalid ICAO format. Must be 6 hex characters"
        )
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            SELECT DISTINCT ON (icao) icao, registration, manufacturer, model, type_code, recorded_time
            FROM aircraft 
            WHERE icao = %s
            ORDER BY icao, recorded_time DESC
            LIMIT 1
            """,
            (icao.upper(),)
        )
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Aircraft with ICAO {icao} not found"
            )
        
        try:
            return Aircraft(**result)
        except ValueError as e:
            raise HTTPException(
                status_code=422, 
                detail=f"Invalid aircraft data: {str(e)}"
            ) from e
            
    except DatabaseConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@router.get("/aircraft/{icao}/co2", response_model=CO2Emission)
async def get_aircraft_co2(icao: str):
    """
    Calculate CO2 emissions for a specific aircraft.
    Uses fuel consumption rates to estimate emissions.
    """
    # Validate ICAO format
    if not re.match(r'^[A-F0-9]{6}$', icao.upper()):
        raise HTTPException(
            status_code=400,
            detail="Invalid ICAO format. Must be 6 hex characters"
        )
    
    try:
        import traceback
        try:
            conn = get_db_connection()
            cur = conn.cursor()
        except Exception as e:
            print(f"Error: {str(e)}\n{traceback.format_exc()}")
            raise
        
        # Get aircraft type and fuel consumption rate
        cur.execute(
            """
            WITH latest_aircraft AS (
                SELECT DISTINCT ON (icao) *
                FROM aircraft
                WHERE icao = %s
                ORDER BY icao, last_updated DESC
            )
            SELECT a.type_code, f.fuel_rate
            FROM latest_aircraft a
            JOIN fuel_consumption f ON a.type_code = f.aircraft_type
            """,
            (icao.upper(),)
        )
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Aircraft not found or no valid fuel consumption data available"
            )
        
        if not result['type_code'] or not result['fuel_rate']:
            raise HTTPException(
                status_code=422,
                detail="Incomplete data: missing aircraft type or fuel consumption rate"
            )
        
        # Calculate CO2 emissions (fuel rate * CO2 factor)
        co2_factor = 3.16  # kg CO2 per kg fuel
        try:
            total_co2 = float(result['fuel_rate']) * co2_factor
            if total_co2 <= 0:
                raise ValueError("CO2 emissions must be positive")
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=422,
                detail=f"Error calculating CO2 emissions: {str(e)}"
            ) from e
        
        return CO2Emission(
            total_co2=total_co2,
            timestamp=datetime.now()
        )
        
    except DatabaseConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        ) from e
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
