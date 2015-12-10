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


cursor2.execute('SELECT MAX(id) FROM language')
last_language_id = cursor2.fetchone()[0] or 0

cursor2.execute('SELECT MAX(id) FROM country')
last_country_id = cursor2.fetchone()[0] or 0

cursor2.execute('SELECT MAX(id) FROM rating')
last_rating_id = cursor2.fetchone()[0] or 0

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

#insert movies to movie

cursor1.execute('SELECT * FROM movie')

print 'start movies'
for row in cursor1.fetchall():
	old_id = row[0]
	adult = row[1]
	budget = row[2]
	homepage = row[3]
	imdb_id = row[4]
	original_title = row[5]
	original_language = row[6]
	description = row[7]
	popularity = row[8]
	production_countries = row[9]
	release_date = row[10]
	revenue = row[11]
	runtime = row[12]
	spoken_languages = row[13]
	status = row[14]
	tagline = row[15]
	title = row[16]
	video = row[17]
	vote_average = row[18]
	vote_count = row[19]

	new_id = "mdb_" + str(old_id)
	id_mapping_movies[old_id] = new_id

	#parse adult
	if adult is not None:
		if adult == 0:
			censor_rating = 'U'
		elif adult == 1:
			censor_rating = 'A'
	else:
		censor_rating = None

	#parse video
	if video is not None:
		if video == 0:
			video = True
		elif video == 1:
			video = False

	#parse release_date	
	if release_date:
		splitted_rdate = release_date.split('-')
		release_day = splitted_rdate[2]
		release_month = splitted_rdate[1]
		release_year = splitted_rdate[0]
	else:
		release_day = None
		release_month = None
		release_year = None

	#map voting attributes to rating table
	if vote_average and vote_count:
		last_rating_id += 1
		rating_id = last_rating_id
		cursor2.execute('INSERT INTO rating(id, average, count) VALUES(%s,%s,%s)', (rating_id, vote_average, vote_count))
	
	cursor2.execute('INSERT INTO movie(id, censor_rating, budget, homepage,imdb_id, original_title, description, popularity, release_day, release_month, release_year, revenue, runtime, status, tagline, title, video, rating_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (new_id, censor_rating, budget, homepage, imdb_id, original_title, description, popularity, release_day, release_month, release_year, revenue, runtime, status, tagline, title, video, rating_id))

	#map original language to language table
	if original_language:
		cursor2.execute('SELECT id FROM language WHERE language = %s', [original_language])
		row = cursor2.fetchone()
		if row is not None:
			language_id = row[0]
		else:
			last_language_id += 1
			language_id = last_language_id
			cursor2.execute('INSERT INTO language VALUES(%s,%s)', [language_id, original_language])
		cursor2.execute('INSERT INTO movie_language(language_id, movie_id, type) VALUES(%s,%s,%s)', [language_id, new_id, 'original'])

	#map spoken languages to language table
	if spoken_languages:
		splitted_slanguage = spoken_languages.split(',')
		for y in range(0, len(splitted_slanguage) - 1):
			language_name = splitted_slanguage[y]
			cursor2.execute('SELECT id FROM language WHERE language = %s', [language_name])
			row = cursor2.fetchone()
			if row is not None:
				language_id = row[0]
			else:
				last_language_id += 1
				language_id = last_language_id
				cursor2.execute('INSERT INTO language VALUES(%s,%s)', (language_id, original_language))
			cursor2.execute('INSERT INTO movie_language(language_id, movie_id, type) VALUES(%s,%s,%s)', [language_id, new_id, 'spoken'])
	
	#map country to country table
	if production_countries:
		splitted_pcountries = production_countries.split(',')
		for i in range(0, len(splitted_pcountries) -1):
			country_name = splitted_pcountries[i]
			cursor2.execute('SELECT id FROM country WHERE name = %s', [country_name])
			row = cursor2.fetchone()
			if row is not None:
				country_id = row[0]
			else:
				last_country_id += 1
				country_id = last_country_id
				cursor2.execute('INSERT INTO country VALUES(%s,%s)', (country_id, country_name))
			cursor2.execute('INSERT INTO movie_country(country_id, movie_id, type) VALUES(%s,%s,%s)', [country_id, new_id, 'production'])

connection2.commit()
print 'end movies'
