from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import boto3
import json
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import pandas as pd
import io

# Load environment variables
load_dotenv()

router = APIRouter()

# Pydantic Models
class Aircraft(BaseModel):
    icao: str = Field(..., description="ICAO address of the aircraft")
    registration: Optional[str] = Field(None, description="Aircraft registration number")
    type_code: Optional[str] = Field(None, description="Aircraft type code")
    manufacturer: Optional[str] = Field(None, description="Aircraft manufacturer")
    model: Optional[str] = Field(None, description="Aircraft model")

class FuelConsumption(BaseModel):
    aircraft_type: str = Field(..., description="Aircraft type")
    fuel_consumption_rate: float = Field(..., description="Fuel consumption rate in kg/hour")

class CO2Emission(BaseModel):
    icao: str = Field(..., description="ICAO address of the aircraft")
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
async def get_aircrafts(conn=Depends(get_db_connection)):
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}") from e

@router.get("/aircraft/{icao}", response_model=Aircraft)
async def get_aircraft(icao: str, conn=Depends(get_db_connection)):
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
                raise HTTPException(status_code=404, detail="Aircraft not found")
            return Aircraft(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}") from e

@router.get("/aircraft/{icao}/co2", response_model=CO2Emission)
async def get_aircraft_co2(icao: str, conn=Depends(get_db_connection)):
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
                raise HTTPException(status_code=404, detail="Aircraft not found")
            aircraft_type = aircraft_result['type_code']
        
        # Get latest fuel consumption rates
        try:
            response = s3_client.get_object(
                Bucket=os.getenv('AWS_BUCKET'),
                Key='prepared/fuel_consumption/latest/data.json'
            )
            fuel_data = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error accessing fuel consumption data: {str(e)}"
            ) from e
        
        # Get flight tracking data for the aircraft
        try:
            # Get the latest date's data
            response = s3_client.get_object(
                Bucket=os.getenv('AWS_BUCKET'),
                Key='prepared/readsb_hist/latest/data.parquet'
            )
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            # Filter for this aircraft
            aircraft_data = df[df['icao'] == icao]
            
            if aircraft_data.empty:
                raise HTTPException(status_code=404, detail="No tracking data found for this aircraft")
            
            # Calculate flight time (in hours)
            flight_time = (aircraft_data['timestamp'].max() - aircraft_data['timestamp'].min()) / 3600
            
            # Get fuel consumption rate for this aircraft type
            fuel_rate = next(
                (item['fuel_consumption_rate'] for item in fuel_data['data'] 
                 if item['aircraft_type'] == aircraft_type),
                None
            )
            
            if not fuel_rate:
                raise HTTPException(status_code=404, detail="Fuel consumption data not found for this aircraft type")
            
            # Calculate CO2 emissions (fuel consumption * flight time * CO2 factor)
            # Using 3.16 as the CO2 emission factor for aviation fuel
            co2_emissions = fuel_rate * flight_time * 3.16
            
            return CO2Emission(
                icao=icao,
                total_co2=co2_emissions,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error processing tracking data: {str(e)}"
            ) from e
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        ) from e
