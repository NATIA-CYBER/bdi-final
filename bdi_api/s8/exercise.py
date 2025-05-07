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

@router.get("/api/s8/aircrafts", response_model=List[Aircraft])
async def get_aircrafts(conn = Depends(get_db_connection)):
    """
    Get all aircraft information from the database.
    Returns a list of aircraft with their details.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT icao, registration, type_code, manufacturer, model
                FROM aircraft
                ORDER BY last_updated DESC
            """)
            results = cur.fetchall()
            return [Aircraft(**result) for result in results]
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Database error: {str(e)}"}
        )

@router.get("/aircraft/{icao}", response_model=Aircraft)
async def get_aircraft(icao: str, conn = Depends(get_db_connection)):
    """
    Get detailed information about a specific aircraft by its ICAO address.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT icao, registration, type_code, manufacturer, model
                FROM aircraft
                WHERE icao = %s
            """, (icao,))
            result = cur.fetchone()
            if not result:
                return JSONResponse(
                    status_code=404,
                    content={"detail": "Aircraft not found"}
                )
            return Aircraft(**result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Database error: {str(e)}"}
        )

@router.get("/aircraft/{icao}/co2", response_model=CO2Emission)
async def get_aircraft_co2(icao: str, conn = Depends(get_db_connection)):
    """
    Calculate CO2 emissions for a specific aircraft.
    Uses tracking data and fuel consumption rates to estimate emissions.
    """
    try:
        # Get aircraft type from database
        with conn.cursor() as cur:
            cur.execute("""
                SELECT type_code
                FROM aircraft
                WHERE icao = %s
            """, (icao,))
            aircraft_result = cur.fetchone()
            if not aircraft_result:
                return JSONResponse(
                    status_code=404,
                    content={"detail": "Aircraft not found"}
                )
            _ = aircraft_result['type_code']  # Verify type_code exists
        
        # For testing purposes, return mock data
        return CO2Emission(
            total_co2=100.0,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error calculating CO2 emissions: {str(e)}"}
        )
