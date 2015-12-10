import psycopg2

connection1 = psycopg2.connect(host='localhost',
	                                   port=5432,
	                                   database='themoviedb',
	                                   user='Rosa')

connection2 = psycopg2.connect(host='localhost',
                                       port=5432,
                                       database='infint_integrated_db',
                                       user='Rosa')

cursor1 = connection1.cursor()
cursor2 = connection2.cursor()

id_mapping_actors = {}
id_mapping_movies = {}

#insert actor in person
cursor1.execute('SELECT * FROM actor')
print 'start persons'
for row in cursor1.fetchall():
	biography = row[2]
	birthday = row[3]
	deathday = row[4]
	homepage = row[5]
	imdb_id = row[6]
	name = row[7]
	place_of_birth = row[8]
	popularity = row[9]
	profile_photo_path = row[10]

	if birthday:
		splitted_birthday = birthday.split('-')
		if len(splitted_birthday) == 3:
			birth_year = splitted_birthday[0]
			birth_month = splitted_birthday[1]
			birth_day = splitted_birthday[2]
		elif len(splitted_birthday)==2:
			birth_day = None
			birth_month = splitted_birthday[1]
			birth_year = splitted_birthday[0]
		elif len(splitted_birthday)==1:
			birth_day = None
			birth_month = None
			birth_year = splitted_birthday[0]
	else:
		birth_day = None
		birth_month = None
		birth_year = None

	if deathday:
		if '-' in deathday:
			splitted_deathday = deathday.split('-')
			if len(splitted_deathday) == 3:
				death_year = splitted_deathday[0]
				death_month = splitted_deathday[1]
				death_day = splitted_deathday[2]
			elif len(splitted_deathday)==2:
				death_day = None
				death_month = splitted_deathday[1]
				death_year = splitted_deathday[0]
			elif len(splitted_deathday)==1:
				death_day = None
				death_month = None
				death_year = splitted_deathday[0]
		elif len(deathday) == 4:
			death_day = None
			death_month = None
			death_year = deathday
		else:
			death_day = None
			death_month = None
			death_year = None
	else:
		death_day = None
		death_month = None
		death_year = None

	if name is not None:
		name = name.strip()
		splitted_name = name.split(' ')
		last_name = splitted_name[len(splitted_name)-1]
		first_name = ''
		for y in range(0, len(splitted_name) -1):
				first_name += splitted_name[y]
				if y != len(splitted_name) -1:
					first_name += ' '

	old_id = row[0]
	new_id = "mdb_" + str(old_id)

	id_mapping_actors[old_id] = new_id

	cursor2.execute('INSERT INTO person(id, first_name, last_name, birth_day, birth_month, birth_year, death_day, death_month,death_year, biography, homepage, imdb_id, place_of_birth, popularity, profile_photo_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (new_id, first_name, last_name, birth_day, birth_month, birth_year, death_day, death_month, death_year, biography, homepage, imdb_id, place_of_birth, popularity, profile_photo_path))
connection2.commit()
print 'end persons'
