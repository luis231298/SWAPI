CREATE DATABASE IF NOT EXISTS star_wars_db;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE dim_people (
id INTEGER PRIMARY KEY,
name VARCHAR(100),
height INTEGER,
mass FLOAT,
homeworld_id INTEGER FOREIGN KEY
);

CREATE TABLE dim_planets (
id INTEGER PRIMARY KEY,
name VARCHAR(100),
rotation_period INTEGER,
orbital_period INTEGER,
diameter INTEGER,
climate VARCHAR(50),
gravity VARCHAR(50),
terrain VARCHAR(50),
surface_water INTEGER,
population BIGINT
);

CREATE TABLE fact_people_films(
character_id INTEGER FOREIGN KEY,
film_id INTEGER
);

CREATE TABLE fact_people_species(
planet_id INTEGER FOREIGN KEY,
resident_id INTEGER
);