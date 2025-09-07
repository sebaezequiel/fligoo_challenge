--CREATING DATABASE
CREATE DATABASE testfligoo;

--CONECTING TO TESTFLIGOO AND CREATING TABLE
\connect testfligoo;

--CREATING TABLE
CREATE TABLE IF NOT EXISTS testdata (
  id SERIAL PRIMARY KEY,
  flight_date DATE,
  flight_status TEXT,
  departure_airport TEXT,
  departure_timezone TEXT,
  arrival_airport TEXT,
  arrival_timezone TEXT,
  arrival_terminal TEXT,
  airline_name TEXT,
  flight_number TEXT,
  ingested_at TIMESTAMP DEFAULT NOW()
);

--CREATING INDEX TO AVOID DUPLICATES
CREATE UNIQUE INDEX IF NOT EXISTS ux_testdata_natural
ON testdata (flight_date, flight_number, airline_name, departure_airport, arrival_airport);