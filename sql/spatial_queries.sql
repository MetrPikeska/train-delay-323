-- SQL and PostGIS queries for spatial operations

-- Example 1: Create a table for railway lines with a GEOMETRY column
-- Assumes you have a PostGIS enabled database.
CREATE TABLE railway_lines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    geom GEOMETRY(LineString, 4326) -- SRID 4326 for WGS84
);

-- Example 2: Insert data into the railway_lines table
-- You would replace 'LINESTRING(lon1 lat1, lon2 lat2, ...)' with actual coordinates
INSERT INTO railway_lines (name, description, geom) VALUES
('Line 323', 'Ostrava - Frydlant - Celadna - Frenstat', ST_SetSRID(ST_GeomFromText('LINESTRING(18.2917 49.8465, 18.3582 49.6645, 18.3615 49.5760, 18.2140 49.5601)'), 4326));

-- Example 3: Create an index on the geometry column for faster spatial queries
CREATE INDEX idx_railway_lines_geom ON railway_lines USING GIST(geom);

-- Example 4: Create a table for train stations with a GEOMETRY column
CREATE TABLE train_stations (
    id SERIAL PRIMARY KEY,
    station_name VARCHAR(255),
    geom GEOMETRY(Point, 4326),
    avg_delay_minutes INT
);

-- Example 5: Insert data into train_stations
INSERT INTO train_stations (station_name, geom, avg_delay_minutes) VALUES
('Ostrava hl.n.', ST_SetSRID(ST_Point(18.2917, 49.8465), 4326), 5),
('Frydlant n.O.', ST_SetSRID(ST_Point(18.3582, 49.6645), 4326), 8),
('Celadna', ST_SetSRID(ST_Point(18.3615, 49.5760), 4326), 3),
('Frenstat p.R.', ST_SetSRID(ST_Point(18.2140, 49.5601), 4326), 10);

-- Example 6: Find all stations within a certain distance of the railway line (e.g., 1000 meters)
-- ST_DWithin performs a distance check, distance is in meters if CRS is projected, or degrees if geographic
-- For SRID 4326 (WGS84), distance units are degrees. Use ST_Transform for meters or use ST_DistanceSphere.
SELECT
    ts.station_name,
    ts.avg_delay_minutes,
    rl.name AS railway_line_name
FROM
    train_stations AS ts,
    railway_lines AS rl
WHERE
    ST_DWithin(ts.geom, rl.geom, 0.01); -- 0.01 degrees is approx 1.1 km at equator

-- A more accurate distance check in meters using ST_Transform (e.g., to a local UTM zone or pseudo-Mercator)
-- For Czech Republic, typically EPSG:5514 or a suitable UTM zone might be used.
-- Using EPSG:3857 (Web Mercator) for broader compatibility, though less precise for small distances.
SELECT
    ts.station_name,
    ts.avg_delay_minutes,
    rl.name AS railway_line_name
FROM
    train_stations AS ts,
    railway_lines AS rl
WHERE
    ST_DWithin(ST_Transform(ts.geom, 3857), ST_Transform(rl.geom, 3857), 1000); -- 1000 meters

-- Example 7: Calculate the length of the railway line
SELECT name, ST_Length(geom::geography) AS length_meters FROM railway_lines;

-- Example 8: Find the nearest station to a given point (e.g., a specific incident location)
SELECT
    station_name,
    ST_Distance(geom, ST_SetSRID(ST_Point(18.30, 49.70), 4326)) AS distance_degrees
FROM
    train_stations
ORDER BY
    distance_degrees
LIMIT 1;

-- Example 9: Aggregate average delays by buffer around stations
-- This query is conceptual and requires more complex setup for actual use with delay data
/*
SELECT
    ST_Buffer(ts.geom, 0.005) AS buffered_area, -- Buffer of 0.005 degrees around station
    AVG(td.delay_minutes) AS average_delay_in_area
FROM
    train_stations ts
JOIN
    train_delays td ON ST_Contains(ST_Buffer(ts.geom, 0.005), td.geom)
GROUP BY
    buffered_area;
*/
