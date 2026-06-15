
CREATE DATABASE IF NOT EXISTS star_wars_db;

CREATE SCHEMA IF NOT EXISTS silver;

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