-- Aircraft table with history tracking
CREATE TABLE IF NOT EXISTS aircraft (
    id SERIAL PRIMARY KEY,
    icao VARCHAR(24),
    registration VARCHAR(255),
    manufacturer VARCHAR(255),
    model VARCHAR(255),
    type_code VARCHAR(255),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(255)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft(icao);
CREATE INDEX IF NOT EXISTS idx_aircraft_last_updated ON aircraft(last_updated);

-- Fuel consumption rates table
CREATE TABLE IF NOT EXISTS fuel_consumption (
    id SERIAL PRIMARY KEY,
    aircraft_type VARCHAR(255),
    fuel_rate DECIMAL(10,2),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for joining
CREATE INDEX IF NOT EXISTS idx_fuel_aircraft_type ON fuel_consumption(aircraft_type);
