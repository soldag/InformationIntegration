import psycopg2
import datetime

connection1 = psycopg2.connect(host='localhost',
	                                   port=5432,
	                                   database='infint_imdb',
	                                   user='Rosa')

connection2 = psycopg2.connect(host='localhost',
                                       port=5432,
                                       database='infinf_in_imdb',
                                       user='Rosa')

cursor1 = connection1.cursor()
cursor2 = connection2.cursor()

id_mapping_actors = {}
id_mapping_movies = {}
id_mapping_directors = {}
id_mapping_cinematgrs = {}
id_mapping_producers = {}
id_mapping_writers = {}
id_mapping_composers = {}

#insert actors in person
cursor1.execute('SELECT * FROM actors')
print 'start persons'
print datetime.datetime.now()

for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		gender = row[2]
		old_id = row[0]
		new_id = "im_a_" + str(old_id)

		id_mapping_actors[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name,gender) VALUES (%s,%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name,gender))
connection2.commit()

print 'end persons'
print datetime.datetime.now()

#insert movies to movie

cursor1.execute('SELECT * FROM movies')

print 'start movies'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_id = row[0]
	title = row[1]
	release_year = row[2]
	if '-' in release_year:
		splitted_year = release_year.split('-')
		if '?' in splitted_year[1]:
			release_year = splitted_year[0]
		else:
			release_year = splitted_year[1]
	if '?' in release_year:
		release_year = None
	imdb_id = row[3]
	new_id = "im_" + str(old_id)

	id_mapping_movies[old_id] = new_id

	cursor2.execute('INSERT INTO movie(id,title,release_year,imdb_id) VALUES (%s,%s,%s,%s)', (new_id,title,release_year,imdb_id))
connection2.commit()

print 'end movies'
print datetime.datetime.now()

#write join table

cursor1.execute('SELECT * FROM movies2actors')

print 'start join table'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_actor_id = row[1]
	role = row[2]
	job_id = 1
	new_movie_id = id_mapping_movies[old_movie_id]
	new_actor_id = id_mapping_actors[old_actor_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, role) VALUES (%s,%s,%s,%s)', (new_actor_id, new_movie_id, job_id, role))
connection2.commit()

print 'end join table'
print datetime.datetime.now()

#insert directors

cursor1.execute('SELECT * FROM directors')
print 'start directors'
print datetime.datetime.now()

for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		old_id = row[0]
		new_id = "im_d_" + str(old_id)

		id_mapping_directors[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name) VALUES (%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name))
connection2.commit()

print 'end directors'
print datetime.datetime.now()

#write join table for directors and movies

cursor1.execute('SELECT * FROM movies2directors')

print 'start join table directors'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_director_id = row[1]
	job_id = 3
	comment = row[2]
	role = ''
	new_movie_id = id_mapping_movies[old_movie_id]
	new_director_id = id_mapping_directors[old_director_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, comment, role) VALUES (%s,%s,%s,%s,%s)', (new_director_id, new_movie_id, job_id, comment, role))
connection2.commit()

print 'end join table directors'
print datetime.datetime.now()

#insert cinematgrs

cursor1.execute('SELECT * FROM cinematgrs')
print 'start cinematgrs'
print datetime.datetime.now()

for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		old_id = row[0]
		new_id = "im_ci_" + str(old_id)

		id_mapping_cinematgrs[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name) VALUES (%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name))
connection2.commit()

print 'end cinematgrs'
print datetime.datetime.now()

#write join table for cinematgrs and movies

cursor1.execute('SELECT * FROM movies2cinematgrs')

print 'start join table cinematgrs'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_cinematgrs_id = row[1]
	job_id = 4
	comment = row[2]
	role = ''
	new_movie_id = id_mapping_movies[old_movie_id]
	new_cinematgrs_id = id_mapping_cinematgrs[old_cinematgrs_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, comment, role) VALUES (%s,%s,%s,%s,%s)', (new_cinematgrs_id, new_movie_id, job_id, comment, role))
connection2.commit()
print 'end join table cinematgrs'
print datetime.datetime.now()

#insert producers

cursor1.execute('SELECT * FROM producers')
print 'start producers'
print datetime.datetime.now()
for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		old_id = row[0]
		new_id = "im_p_" + str(old_id)

		id_mapping_producers[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name) VALUES (%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name))
connection2.commit()

print 'end producers'
print datetime.datetime.now()

#write join table for producers and movies

cursor1.execute('SELECT * FROM movies2producers')

print 'start join table producers'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_producers_id = row[1]
	job_id = 2
	comment = row[2]
	role = ''
	new_movie_id = id_mapping_movies[old_movie_id]
	new_producers_id = id_mapping_producers[old_producers_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, comment, role) VALUES (%s,%s,%s,%s,%s)', (new_producers_id, new_movie_id, job_id, comment, role))
connection2.commit()
print 'end join table producers'
print datetime.datetime.now()

#insert writers

cursor1.execute('SELECT * FROM writers')
print 'start writers'
print datetime.datetime.now()

for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		old_id = row[0]
		new_id = "im_w_" + str(old_id)

		id_mapping_writers[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name) VALUES (%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name))
connection2.commit()

print 'end writers'
print datetime.datetime.now()

#write join table for writers and movies

cursor1.execute('SELECT * FROM movies2writers')

print 'start join table writers'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_writers_id = row[1]
	job_id = 6
	comment = row[2]
	role = ''
	new_movie_id = id_mapping_movies[old_movie_id]
	new_writers_id = id_mapping_writers[old_writers_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, comment, role) VALUES (%s,%s,%s,%s,%s)', (new_writers_id, new_movie_id, job_id, comment,role))
connection2.commit()
print 'end join table writers'
print datetime.datetime.now()

#insert composers

cursor1.execute('SELECT * FROM composers')
print 'start composers'
print datetime.datetime.now()

for row in cursor1.fetchall():
	if row[1] is not None:
		splitted_name = row[1].split(', ')
		if len(splitted_name) == 1:
			last_name = splitted_name[0]
			first_name = None
			stage_name = None
		elif ' ' not in splitted_name[0]:
			first_name = splitted_name[1]
			last_name = splitted_name[0]
			stage_name = None
		else:
			first_name = splitted_name[len(splitted_name)-1]
			second_splitted_name = splitted_name[0].split()
			last_name = second_splitted_name[len(second_splitted_name)-1]
			stage_name = ' '.join(second_splitted_name[:-1])
			stage_name = stage_name.replace('\'', '')
		old_id = row[0]
		new_id = "im_co_" + str(old_id)

		id_mapping_composers[old_id] = new_id

		cursor2.execute('INSERT INTO person(id,first_name,last_name,stage_name) VALUES (%s,%s,%s,%s)', (new_id,first_name,last_name,stage_name))
connection2.commit()

print 'end composers'
print datetime.datetime.now()

#write join table for composers and movies

cursor1.execute('SELECT * FROM movies2writers')

print 'start join table writers'
print datetime.datetime.now()

for row in cursor1.fetchall():
	old_movie_id = row[0]
	old_composers_id = row[1]
	job_id = 5
	comment = row[2]
	role = ''
	new_movie_id = id_mapping_movies[old_movie_id]
	new_composers_id = id_mapping_composers[old_composers_id]

	cursor2.execute('INSERT INTO person_movie(person_id, movie_id, job_id, comment, role) VALUES (%s,%s,%s,%s,%s)', (new_composers_id, new_movie_id, job_id, comment, role))
connection2.commit()
print 'end join table composers'
print datetime.datetime.now()


