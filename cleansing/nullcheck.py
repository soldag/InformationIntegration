from __future__ import generators
import psycopg2
import datetime
import re

connection = psycopg2.connect(host='localhost',
								port=5432,
                                user='postgres',
                                password='postgres',
                                database='integrated')
cursor = connection.cursor()
cursor2 = connection.cursor()
arraysize = 1000

def check_attributes(row, check_unknown = True):
	new_attributes = {}
	for i in range(1, len(row)):
		if row[i] == '' or row[i] == ' ':
			new_attributes[i] = None
		elif check_unknown and row[i] == 'unknown':
			new_attributes[i] = None
		else:
			new_attributes[i] = row[i]
	return new_attributes

def check_goodreads_strings(row):
	new_attributes = {}
	for i in range(1, len(row)):
		matched = re.match( r'.*[\?].*[\?].*$', str(row[i]), re.M|re.I)
		if matched:
			new_attributes[i] = re.sub(r'[^A-Za-z0-9]*\?[\?\!](\?)+[^A-Za-z0-9]*', '', str(row[i]))
			new_attributes[i] = re.sub(r'\((\?)+\)', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\(\)', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\A\ \-\ ', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\A(\?\.)+', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\b', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\A\?\?$', '', str(new_attributes[i]))
			new_attributes[i] = re.sub(r'\A\?\?\ \-', '', str(new_attributes[i]))
		else:
			new_attributes[i] = row[i]
	return new_attributes

print 'start artist_credit_name'
print datetime.datetime.now()

arraysize = 500
cursor.execute('SELECT COUNT(artist_credit_id) FROM artist_credit_name')
rows = cursor.fetchone()[0]

cursor.execute('SELECT artist_credit_id, person_id, name FROM artist_credit_name')

i = 0
while (i <= rows and arraysize != 0 and rows != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
    	artist_credit_id = row[0]
    	person_id = row[1]
    	new_a = check_attributes(row)

    	cursor2.execute('UPDATE artist_credit_name SET name=%s WHERE artist_credit_id=%s AND person_id=%s',
                                        [new_a[2], artist_credit_id, person_id])
    	i = i + 1
    	if rows - i < arraysize:
			arraysize = rows - i

print 'end artist_credit_name'
print datetime.datetime.now()

print 'start country'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM country')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, name FROM country')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE country SET name=%s WHERE id=%s',
									[new_a[1], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end country'
print datetime.datetime.now()

print 'start job'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM job')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, name FROM job')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE job SET name=%s WHERE id=%s',
                                        [new_a[1], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end job'
print datetime.datetime.now()

print 'start language'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM language')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, language FROM language')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE language SET language=%s WHERE id=%s',
                                        [new_a[1], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end language'
print datetime.datetime.now()

print 'start location'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM location')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, name FROM location')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE location SET name=%s WHERE id=%s',
                                        [new_a[1], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end location'
print datetime.datetime.now()

print 'start movie'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM movie')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, imdb_id, title, original_title, description, region, genre, censor_rating, poster_path, homepage, status, tagline, studios, process_used_to_produce, awards, comment FROM movie')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE movie SET imdb_id=%s, title=%s, original_title=%s, description=%s, region=%s, genre=%s, censor_rating=%s, poster_path=%s, homepage=%s, status=%s, tagline=%s, studios=%s, process_used_to_produce=%s, awards=%s, comment=%s WHERE id=%s',
                                        [new_a[1], new_a[2], new_a[3], new_a[4], new_a[5], new_a[6], new_a[7], new_a[8], new_a[9], new_a[10], new_a[11], new_a[12], new_a[13], new_a[14], new_a[15], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end movie'
print datetime.datetime.now()

print 'start person'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM person')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, first_name, last_name, stage_name, comment, profile_photo_path, biography, homepage, imdb_id, place_of_birth, place_of_death, photo, occupation FROM person')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)
		new_a = check_goodreads_strings(row)

		cursor2.execute('UPDATE person SET first_name=%s,last_name=%s,stage_name=%s, comment=%s, profile_photo_path=%s, biography=%s, homepage=%s, imdb_id=%s, place_of_birth=%s, place_of_death=%s, photo=%s, occupation=%s WHERE id=%s',
                                        [new_a[1], new_a[2], new_a[3], new_a[4], new_a[5], new_a[6], new_a[7], new_a[8], new_a[9], new_a[10], new_a[11], new_a[12], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end person'
print datetime.datetime.now()

print 'start person_movie'
print datetime.datetime.now()

cursor3 = connection.cursor('cursor3')
cursor3.itersize = 100000 # Rows fetched at one time from the server

arraysize = 1000
cursor.execute('SELECT COUNT(person_id) FROM person_movie')
rows = cursor.fetchone()[0]

cursor3.execute('SELECT person_id, movie_id, job_id, awards, role, role_type, comment FROM person_movie')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor3.fetchmany(arraysize)
    for row in results:
		person_id = row[0]
		movie_id = row[1]
		job_id = row[2]
		role = row[4]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE person_movie SET awards=%s, role_type=%s, comment=%s WHERE person_id=%s AND movie_id=%s AND job_id=%s AND role=%s',
                                        [new_a[3], new_a[5], new_a[6], person_id, movie_id, job_id, role])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end person_movie'
print datetime.datetime.now()

print 'start person_relations'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(person1) FROM person_relations')
rows = cursor.fetchone()[0]

cursor.execute('SELECT person1, person2, type FROM person_relations')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		person1 = row[0]
		person2 = row[1]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE person_relations SET type=%s WHERE person1=%s AND person2=%s',
                                        [new_a[2], person1, person2])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end person_relations'
print datetime.datetime.now()

print 'start release'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM release')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, barcode FROM release')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE release SET barcode=%s WHERE id=%s',
                                        [new_a[1], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end release'
print datetime.datetime.now()

print 'start trailer'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM trailer')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, link, embed_code FROM trailer')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)

		cursor2.execute('UPDATE trailer SET link=%s, embed_code=%s WHERE id=%s',
                                        [new_a[1], new_a[2], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end trailer'
print datetime.datetime.now()

print 'start work'
print datetime.datetime.now()

arraysize = 1000
cursor.execute('SELECT COUNT(id) FROM work')
rows = cursor.fetchone()[0]

cursor.execute('SELECT id, title, description, edition_information, format, isbn, isbn13, publisher FROM work')

i = 0
while (i <= rows and arraysize != 0) :
    results = cursor.fetchmany(arraysize)
    for row in results:
		id = row[0]
		new_a = check_attributes(row)
		new_a = check_goodreads_strings(row)

		cursor2.execute('UPDATE work SET title=%s, description=%s, edition_information=%s, format=%s, isbn=%s, isbn13=%s, publisher=%s WHERE id=%s',
                                        [new_a[1], new_a[2], new_a[3], new_a[4], new_a[5], new_a[6], new_a[7], id])
		i = i + 1
		if rows - i < arraysize:
			arraysize = rows - i

print 'end work'
print datetime.datetime.now()

connection.commit()
connection.close()