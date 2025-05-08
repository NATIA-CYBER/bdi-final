import os
import re
from datetime import datetime
from typing import List, Optional

import boto3
import psycopg2
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException
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
    icao: str = Field(..., description="ICAO address of the aircraft", min_length=6, max_length=6)
    registration: Optional[str] = Field(
        None, description="Aircraft registration number", max_length=20
    )
    type_code: Optional[str] = Field(None, description="Aircraft type code", max_length=10)
    manufacturer: Optional[str] = Field(None, description="Aircraft manufacturer", max_length=100)
    model: Optional[str] = Field(None, description="Aircraft model", max_length=100)

    @validator('icao')
    def validate_icao(cls, v):
        if not re.match(r'^[A-F0-9]{6}$', v):
            raise ValueError('ICAO address must be 6 characters long and contain only hexadecimal digits')
        return v

    @validator('registration')
    def validate_registration(cls, v):
        if v and not re.match(r'^[A-Z0-9-]+$', v):
            raise ValueError('Registration must contain only uppercase letters, numbers, and hyphens')
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
            host='localhost',
            database='airflow',
            user='airflow',
            password='airflow',
            cursor_factory=RealDictCursor
        )
        return conn
    except psycopg2.Error as e:
        raise DatabaseConnectionError(f"Failed to connect to database: {str(e)}")

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
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get latest data for first 100 aircraft
        cur.execute("""
            SELECT DISTINCT ON (icao)
                icao, registration, manufacturer, model, type_code
            FROM aircraft
            WHERE icao IS NOT NULL
              AND icao ~ '^[A-F0-9]{6}$'
            ORDER BY icao, recorded_time DESC
            LIMIT 100
        """)
        
        results = cur.fetchall()
        
        # Validate each aircraft record
        validated_aircraft = []
        for aircraft in results:
            try:
                validated = Aircraft(**aircraft)
                validated_aircraft.append(validated)
            except ValueError as e:
                # Log invalid data but continue processing
                print(f"Invalid aircraft data: {aircraft}, Error: {str(e)}")
        
        return validated_aircraft
        
    except DatabaseConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
            detail="Invalid ICAO address format. Must be 6 characters long and contain only hexadecimal digits"
        )
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT icao, registration, manufacturer, model, type_code
            FROM aircraft
            WHERE icao = %s
            ORDER BY recorded_time DESC
            LIMIT 1
        """, (icao.upper(),))
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Aircraft with ICAO {icao} not found"
            )
        
        try:
            return Aircraft(**result)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=f"Invalid aircraft data: {str(e)}")
            
    except DatabaseConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
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
            detail="Invalid ICAO address format. Must be 6 characters long and contain only hexadecimal digits"
        )
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get aircraft type and fuel consumption rate
        cur.execute("""
            SELECT a.type_code, f.fuel_rate
            FROM aircraft a
            JOIN fuel_consumption f ON a.type_code = f.aircraft_type
            WHERE a.icao = %s
              AND a.type_code IS NOT NULL
              AND f.fuel_rate > 0
            ORDER BY a.recorded_time DESC
            LIMIT 1
        """, (icao.upper(),))
        
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
            )
        
        return CO2Emission(
            total_co2=total_co2,
            timestamp=datetime.now()
        )
        
    except DatabaseConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
