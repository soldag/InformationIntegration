CREATE TABLE persons (
        id VARCHAR(16) PRIMARY KEY NOT NULL,
        name VARCHAR(256),
        birth_name VARCHAR(256),
        life_span VARCHAR(64),
        place_of_birth VARCHAR(16),
        place_of_death VARCHAR(16),
        gender CHAR,
        occupation VARCHAR(256)
    ); 

CREATE TABLE person_relations (
        person1 VARCHAR(16) NOT NULL,
        person2 VARCHAR(16) NOT NULL,
        type VARCHAR(256) NOT NULL, 
        PRIMARY KEY(person1, person2, type)
    );

CREATE TABLE works(
        id VARCHAR(16) PRIMARY KEY NOT NULL,
        name VARCHAR(256)
    );

CREATE TABLE work_authors (
        work VARCHAR(16) NOT NULL,
        author VARCHAR(16) NOT NULL, 
        PRIMARY KEY(work, author)
    ); 

CREATE TABLE places (
        id VARCHAR(16) PRIMARY KEY NOT NULL,
        name VARCHAR(256),
        latitude VARCHAR(16),
        longitude VARCHAR(16)
    );
