--Creación de Base de datos y esquema
CREATE DATABASE IF NOT EXISTS star_wars_db;

CREATE SCHEMA IF NOT EXISTS silver;

--Creación de Tablas separadas por DIM y FACTS.
CREATE TABLE silver.dim_planets (
id INTEGER PRIMARY KEY,
name VARCHAR(100),
rotation_period INTEGER,
orbital_period INTEGER,
diameter INTEGER,
climate VARCHAR(100),
gravity VARCHAR(100),
terrain VARCHAR(100),
surface_water INTEGER,
population BIGINT
);

CREATE TABLE silver.dim_people (
id INTEGER PRIMARY KEY,
name VARCHAR(100),
height INTEGER,
mass FLOAT,
homeworld_id INTEGER REFERENCES silver.dim_planets(id)
);

CREATE TABLE silver.fact_people_films(
character_id INTEGER REFERENCES silver.dim_people(id),
film_id INTEGER
);

CREATE TABLE silver.fact_people_species(
character_id INTEGER REFERENCES silver.dim_people(id),
species_id INTEGER
);

CREATE TABLE silver.fact_planet_residents(
planet_id INTEGER REFERENCES silver.dim_planets(id),
resident_id INTEGER REFERENCES silver.dim_people(id)
);

--Creación de usuarios y dandole los permisos necesarios
CREATE USER etl_user WITH PASSWORD 'etl123';
GRANT CONNECT ON DATABASE star_wars_db TO etl_user;
GRANT USAGE,CREATE ON SCHEMA silver TO etl_user;

-- Permisos para las tablas que YA existen hoy
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA silver TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO etl_user;

--Tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA silver 
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver 
GRANT USAGE, SELECT ON SEQUENCES TO etl_user;

CREATE USER dashboard_user WITH PASSWORD 'dash123';
GRANT CONNECT ON DATABASE star_wars_db TO dashboard_user;
GRANT USAGE ON SCHEMA silver TO dashboard_user;

-- Permisos para las tablas que YA existen hoy 
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO dashboard_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA silver TO dashboard_user;

--Tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA silver 
GRANT SELECT ON TABLES TO dashboard_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver 
GRANT SELECT ON SEQUENCES TO dashboard_user;