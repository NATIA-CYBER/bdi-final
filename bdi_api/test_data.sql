-- Insert test aircraft data
INSERT INTO aircraft (icao, registration, manufacturer, model, type_code, source) VALUES
('ABC123', 'N123AB', 'Boeing', '737-800', 'B738', 'test'),
('XYZ789', 'G-ABCD', 'Airbus', 'A320-200', 'A320', 'test');

-- Insert test fuel consumption data
INSERT INTO fuel_consumption (aircraft_type, fuel_rate) VALUES
('B738', 2500.5),  -- Boeing 737-800
('A320', 2100.3);  -- Airbus A320
