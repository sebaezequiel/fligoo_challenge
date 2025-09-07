CREATE TABLE IF NOT EXISTS testdata (
  flight_date date NOT NULL,
  flight_status text,
  departure_airport text NOT NULL,
  departure_timezone text,
  arrival_airport text NOT NULL,
  arrival_timezone text,
  arrival_terminal text,
  airline_name text NOT NULL,
  flight_number text NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_testdata_natural
  ON testdata (flight_date, flight_number, airline_name, departure_airport, arrival_airport);
