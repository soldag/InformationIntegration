-- ALTER DATABASE infint_baseball SET datestyle TO "ISO, MDY";

CREATE TABLE Master (
    playerID VARCHAR(10) PRIMARY KEY,
    birthYear INTEGER,
    birthMonth INTEGER,
    birthDay INTEGER,
    birthCountry VARCHAR(50),
    birthState VARCHAR(30),
    birthCity VARCHAR(50),
    deathYear INTEGER,
    deathMonth INTEGER,
    deathDay INTEGER,
    deathCountry VARCHAR(50),
    deathState VARCHAR(20),
    deathCity VARCHAR(50),
    nameFirst VARCHAR(50),
    nameLast VARCHAR(50),
    nameGiven VARCHAR(255),
    weight INTEGER,
    height DOUBLE PRECISION,
    bats VARCHAR(1),
    throws VARCHAR(1),
    debut DATE,
    finalGame DATE,
    retroID VARCHAR(9),
    bbrefID VARCHAR(9)
);

CREATE TABLE HallOfFame (
    playerID VARCHAR(10),
    yearid INTEGER,
    votedBy VARCHAR(64),
    ballots INTEGER,
    needed INTEGER,
    votes INTEGER,
    inducted VARCHAR(1),
    category VARCHAR(20),
    needed_note VARCHAR(25),
    PRIMARY KEY ( playerID, yearid, votedBy )
);

CREATE TABLE Schools (
    schoolID VARCHAR(15) PRIMARY KEY,
    name_full VARCHAR(255),
    city VARCHAR(55),
    state VARCHAR(55),
    country VARCHAR(55)
);

CREATE TABLE CollegePlaying (
    playerID VARCHAR(9),
    schoolID VARCHAR(15),
    yearID INTEGER
);

CREATE TABLE Salaries (
    yearID INTEGER,
    teamID VARCHAR(3),
    lgID VARCHAR(2),
    playerID VARCHAR(9),
    salary DOUBLE PRECISION,
    PRIMARY KEY ( yearID, teamID, lgID, playerID )
);

COPY Master FROM 'Master.csv' DELIMITER ',' csv HEADER;
COPY Salaries FROM 'Salaries.csv' DELIMITER ',' csv HEADER;
COPY HallOfFame FROM 'HallOfFame.csv' DELIMITER ',' csv HEADER;
COPY Schools FROM 'Schools.csv' DELIMITER ',' csv HEADER;
COPY CollegePlaying FROM 'CollegePlaying.csv' DELIMITER ',' csv HEADER;

DELETE FROM HallOfFame WHERE playerID NOT IN (SELECT playerID FROM Master);
DELETE FROM CollegePlaying WHERE playerID NOT IN (SELECT playerID FROM Master);
DELETE FROM Salaries WHERE playerID NOT IN (SELECT playerID FROM Master);
DELETE FROM CollegePlaying WHERE schoolID NOT IN (SELECT schoolID FROM Schools);

ALTER TABLE HallOfFame ADD CONSTRAINT foreignPlayerIDHallOfFame FOREIGN KEY (playerID) REFERENCES master(playerID);
ALTER TABLE CollegePlaying ADD CONSTRAINT foreignPlayerIDCollegePlaying FOREIGN KEY (playerID) REFERENCES master(playerID);
ALTER TABLE Salaries ADD CONSTRAINT foreignPlayerIDSalaries FOREIGN KEY (playerID) REFERENCES master(playerID);
ALTER TABLE CollegePlaying ADD CONSTRAINT foreignSchoolIDCollegePlaying FOREIGN KEY (schoolID) REFERENCES Schools(schoolID);
