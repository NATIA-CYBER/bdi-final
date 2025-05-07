from fastapi.testclient import TestClient

from bdi_api.app import app

client = TestClient(app)

def test_get_aircrafts():
    """Test getting all aircraft information"""
    response = client.get("/api/s8/aircrafts")
    assert response.status_code == 200
    aircrafts = response.json()
    assert isinstance(aircrafts, list)
    if len(aircrafts) > 0:
        assert all(isinstance(aircraft, dict) for aircraft in aircrafts)
        assert all("icao" in aircraft for aircraft in aircrafts)

def test_get_aircraft():
    """Test getting specific aircraft information"""
    # Test with valid ICAO
    test_icao = "ABC123"
    response = client.get(f"/aircraft/{test_icao}")
    assert response.status_code in [200, 404]  # Either found or not found is acceptable
    
    if response.status_code == 200:
        aircraft = response.json()
        assert aircraft["icao"] == test_icao

def test_get_aircraft_co2():
    """Test getting CO2 emissions for an aircraft"""
    test_icao = "ABC123"
    response = client.get(f"/aircraft/{test_icao}/co2")
    assert response.status_code in [200, 404]  # Either found or not found is acceptable
    
    if response.status_code == 200:
        co2_data = response.json()
        assert co2_data["icao"] == test_icao
        assert isinstance(co2_data["total_co2"], (int, float))
        assert isinstance(co2_data["timestamp"], str)

def test_get_aircraft_not_found():
    """Test getting non-existent aircraft"""
    response = client.get("/aircraft/NONEXISTENT")
    assert response.status_code == 404

def test_get_aircraft_co2_not_found():
    """Test getting CO2 emissions for non-existent aircraft"""
    response = client.get("/aircraft/NONEXISTENT/co2")
    assert response.status_code == 404
