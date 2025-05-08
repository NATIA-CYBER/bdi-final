import os
from datetime import datetime
from typing import List, Optional

import boto3
import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv()

router = APIRouter()

# Pydantic Models
class Aircraft(BaseModel):
    icao: str = Field(..., description="ICAO address of the aircraft")
    registration: Optional[str] = Field(
        None, description="Aircraft registration number"
    )
    type_code: Optional[str] = Field(None, description="Aircraft type code")
    manufacturer: Optional[str] = Field(None, description="Aircraft manufacturer")
    model: Optional[str] = Field(None, description="Aircraft model")

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
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()

# S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

@router.get("/aircrafts", response_model=List[Aircraft])
async def get_aircrafts():
    """
    Get all aircraft information from the database.
    Returns a list of aircraft with their details.
    """
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get latest data for first 100 aircraft
        cur.execute("""
            SELECT DISTINCT ON (icao)
                icao, registration, manufacturer, model, type_code
            FROM aircraft
            ORDER BY icao, recorded_time DESC
            LIMIT 100
        """)
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return [Aircraft(**aircraft) for aircraft in results]
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )

@router.get("/aircraft/{icao}", response_model=Aircraft)
async def get_aircraft(icao: str):
    """
    Get detailed information about a specific aircraft by its ICAO address.
    """
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT icao, registration, manufacturer, model, type_code
            FROM aircraft
            WHERE icao = %s
            ORDER BY recorded_time DESC
            LIMIT 1
        """, (icao,))
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if not result:
            return JSONResponse(
                status_code=404,
                content={"detail": f"Aircraft with ICAO {icao} not found"}
            )
            
        return Aircraft(**result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )

@router.get("/aircraft/{icao}/co2", response_model=CO2Emission)
async def get_aircraft_co2(icao: str):
    """
    Calculate CO2 emissions for a specific aircraft.
    Uses fuel consumption rates to estimate emissions.
    """
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get aircraft type and fuel consumption rate
        cur.execute("""
            SELECT a.type_code, f.fuel_rate
            FROM aircraft a
            JOIN fuel_consumption f ON a.type_code = f.aircraft_type
            WHERE a.icao = %s
            ORDER BY a.recorded_time DESC
            LIMIT 1
        """, (icao,))
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if not result:
            return JSONResponse(
                status_code=404,
                content={"detail": "Aircraft not found or no fuel consumption data available"}
            )
        
        # Calculate CO2 emissions (fuel rate * CO2 factor)
        co2_factor = 3.16  # kg CO2 per kg fuel
        total_co2 = float(result['fuel_rate']) * co2_factor
        
        return CO2Emission(
            total_co2=total_co2,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )
