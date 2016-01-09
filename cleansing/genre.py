from __future__ import division
import math
from os import dup
import psycopg2

def insert_into_genre_and_join_table(genre, movie_id, next_genre_id):
	cursor.execute('SELECT id,genre FROM genre WHERE genre = %s', [genre])
	existing_genre = cursor.fetchone()
	# insert into genre table
	if existing_genre is None:
		next_genre_id += 1
		genre_id = next_genre_id
		cursor.execute('INSERT INTO genre(id, genre) VALUES(%s,%s)', [genre_id, genre])
	else:
		genre_id = existing_genre[0]
	# insert into join table
	cursor.execute('INSERT INTO movie_genre(movie_id, genre_id) VALUES(%s,%s)', [movie_id, genre_id])
	return next_genre_id

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='Rosa',
                              database='infint_integrated')
cursor = connection.cursor()

cursor.execute('DROP TABLE movie_genre')
cursor.execute('DROP TABLE genre')
connection.commit()

cursor.execute('CREATE TABLE genre (id character varying NOT NULL, genre character varying(50))')
cursor.execute('CREATE TABLE movie_genre (movie_id character varying, genre_id character varying)')
cursor.execute('ALTER TABLE ONLY genre ADD CONSTRAINT genre_pkey PRIMARY KEY (id)')
cursor.execute('ALTER TABLE ONLY movie_genre ADD CONSTRAINT movie_genre_pkey PRIMARY KEY (movie_id,genre_id)')
cursor.execute('ALTER TABLE ONLY movie_genre ADD CONSTRAINT movie_genre_genre_id_fkey FOREIGN KEY (genre_id) REFERENCES genre(id)')
cursor.execute('ALTER TABLE ONLY movie_genre ADD CONSTRAINT movie_genre_movie_id_fkey FOREIGN KEY (movie_id) REFERENCES movie(id)')
connection.commit()

cursor.execute('SELECT id,genre FROM movie WHERE id NOT LIKE \'uci_%\'')

next_genre_id = 0
for row in cursor.fetchall():
	movie_id = row[0]
	genre = row[1]
	if genre is not None:
		next_genre_id = insert_into_genre_and_join_table(genre, movie_id, next_genre_id)


cursor.execute('SELECT id,genre FROM movie WHERE id LIKE \'uci_%\'')
for row in cursor.fetchall():
	movie_id = row[0]
	genre = row[1]
	#parse genre 
	if genre is not None:
		genres = genre.split(';')
		for element in genres:
			element = element.strip()
			element = element.upper()
			if element == 'ACTN':
				genre = 'Violence'
			elif element == 'ADVT':
				genre = 'Adventure'
			elif element == 'DOCU':
				genre = 'Documentary'
			elif element == 'DRAM' or element == 'DRM':
				genre = 'Drama'
			elif element == 'COMD':
				genre = 'Comedy'
			elif element == 'CNR':
				genre = 'Cops and Robbers'
			elif element == 'MUSC' or element == 'MUSCL':
				genre = 'Musical'
			elif element == 'ROMT':
				genre = 'Romantic'
			elif element == 'CART':
				genre = 'Cartoon'
			elif element == 'DISA':
				genre = 'Disaster'
			elif element == 'MYST':
				genre = 'Mystery'
			elif element == 'PORN' or element == 'PORM':
				genre = 'Pornography'
			elif element == 'FANT':
				genre = 'Fantasy'
			elif element == 'HORR':
				genre = 'Horror'
			elif element == 'EPIC':
				genre = 'Epic'
			elif element == 'HIST':
				genre = 'History'
			elif element == 'SCFI':
				genre = 'Science Fiction'
			elif element == 'SUSP':
				genre = 'Thriller'
			elif element == 'NOIR':
				genre = 'Black'
			elif element == 'WEST':
				genre = 'Western'
			elif element == 'BIOP':
				genre = 'Biography'
			elif element == 'CTXX':
				genre = None
			elif element == '\TCOL':
				genre = None
			elif element == 'BNW':
				genre = None
			elif element == 'H' or element == 'H*':
				genre = None
			elif element == 'RFP':
				genre = None
			elif element == 'CROMT':
				genre = None
			elif element == 'AW':
				genre = None
			elif element == 'HOMO':
				genre = None
			elif element == '|':
				genre = None
			if genre is not None:
				next_genre_id = insert_into_genre_and_join_table(genre, movie_id, next_genre_id)
connection.commit()

#delete attribute genre from table movie
cursor.execute('ALTER TABLE movie DROP COLUMN genre')
connection.commit()
