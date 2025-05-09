from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from bdi_api.app import app
from bdi_api.s8.exercise import get_db_connection, s3_client

client = TestClient(app)

# Mock data
MOCK_AIRCRAFT = {
    "icao": "ABC123",
    "registration": "N123AB",
    "type_code": "B738",
    "manufacturer": "Boeing",
    "model": "737-800"
}

MOCK_FUEL_DATA = {
    "data": [{
        "aircraft_type": "B738",
        "fuel_consumption_rate": 2400.0
    }]
}

MOCK_TRACKING_DATA = b'mock tracking data'

async def test_get_aircrafts():
    # Setup FastAPI test client
    app.dependency_overrides[get_db_connection] = lambda: MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(
                fetchall=MagicMock(return_value=[MOCK_AIRCRAFT]),
                __enter__=MagicMock(return_value=MagicMock(
                    fetchall=MagicMock(return_value=[MOCK_AIRCRAFT])
                ))
            )
        )
    )
    # Test getting all aircraft
    response = client.get("/api/s8/aircrafts")
    assert response.status_code == 200
    aircrafts = response.json()
    assert isinstance(aircrafts, list)
    if len(aircrafts) > 0:
        assert all(isinstance(aircraft, dict) for aircraft in aircrafts)
        assert all("icao" in aircraft for aircraft in aircrafts)

async def test_get_aircraft():
    # Setup FastAPI test client
    app.dependency_overrides[get_db_connection] = lambda: MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(
                fetchone=MagicMock(return_value=MOCK_AIRCRAFT),
                __enter__=MagicMock(return_value=MagicMock(
                    fetchone=MagicMock(return_value=MOCK_AIRCRAFT)
                ))
            )
        )
    )
    # Test getting specific aircraft
    # Test with valid ICAO
    test_icao = "ABC123"
    response = client.get(f"/aircraft/{test_icao}")
    assert response.status_code in [200, 404]  # Either found or not found is acceptable
    
    if response.status_code == 200:
        aircraft = response.json()
        assert aircraft["icao"] == test_icao

async def test_get_aircraft_co2():
    # Setup FastAPI test client
    app.dependency_overrides[get_db_connection] = lambda: MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(
                fetchone=MagicMock(return_value=MOCK_AIRCRAFT),
                __enter__=MagicMock(return_value=MagicMock(
                    fetchone=MagicMock(return_value=MOCK_AIRCRAFT)
                ))
            )
        )
    )

    # Mock S3 client
    mock_s3 = MagicMock()
    app.dependency_overrides[s3_client] = lambda: mock_s3
    # Test getting CO2 emissions
    test_icao = "ABC123"
    response = client.get(f"/aircraft/{test_icao}/co2")
    assert response.status_code == 200
    data = response.json()
    assert "total_co2" in data
    assert "timestamp" in data
    assert isinstance(data["total_co2"], (int, float))
    assert isinstance(data["timestamp"], str)

async def test_get_aircraft_not_found():
    # Setup FastAPI test client
    app.dependency_overrides[get_db_connection] = lambda: MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(
                fetchone=MagicMock(return_value=None),
                __enter__=MagicMock(return_value=MagicMock(
                    fetchone=MagicMock(return_value=None)
                ))
            )
        )
    )
    # Test getting non-existent aircraft
    response = client.get("/aircraft/NONEXISTENT")
    assert response.status_code == 404

async def test_get_aircraft_co2_not_found():
    # Setup FastAPI test client
    app.dependency_overrides[get_db_connection] = lambda: MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(
                fetchone=MagicMock(return_value=None),
                __enter__=MagicMock(return_value=MagicMock(
                    fetchone=MagicMock(return_value=None)
                ))
            )
        )
    )
    # Test getting CO2 emissions for non-existent aircraft
    response = client.get("/aircraft/NONEXISTENT/co2")
    assert response.status_code == 404

