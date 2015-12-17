import psycopg2
import datetime

def parse_to_date_object(day,month,year):
	if day is not None and month is not None and year is not None:
		date = datetime.date(year,month,day)
	elif day is None and month is not None and year is not None:
		day = 1 
		date = datetime.date(year,month,day)
	elif day is None and month is None and year is not None:
		day = 1
		month = 1
		date = datetime.date(year,month,day)
	else:
		date = None
	return date 

connection = psycopg2.connect(host='localhost',
									port=5432,
									database='infint_integrated',
									user='Rosa')

cursor = connection.cursor()

cursor.execute('SELECT birth_day, birth_month, birth_year, death_day, death_month, death_year, id FROM person')

for row in cursor.fetchall():
	birth_day = row[0]
	birth_month = row[1]
	birth_year = row[2]
	death_day = row[3]
	death_month = row[4]
	death_year = row[5]
	id = row[6]
	try:
		birth_date = parse_to_date_object(birth_day,birth_month,birth_year)
	except ValueError as e: 
		if str(e) == 'day is out of range for month':
			birth_day = None
			birth_date = parse_to_date_object(birth_day,birth_month,birth_year)
		elif str(e) == 'month must be in 1..12':
			birth_month = None
			birth_date = parse_to_date_object(birth_day,birth_month,birth_year)
	try:
		death_date = parse_to_date_object(death_day,death_month,death_year)
	except ValueError as e:
		if str(e) == 'day is out of range for month':
			death_day = None
			death_date = parse_to_date_object(death_day,death_month,death_year)
		elif str(e) == 'month must be in 1..12':
			death_month = None
			death_date = parse_to_date_object(death_day,death_month,death_year)
	if birth_date is not None and death_date is not None:
		if birth_date > death_date:
			birth_day = None
			birth_month = None
			birth_year = None
			death_day = None
			death_month = None
			death_year = None
	if birth_day != row[0]:
		cursor.execute('UPDATE person SET birth_day=%s WHERE id = %s', [birth_day,id])
	if birth_month != row[1]:
		cursor.execute('UPDATE person SET birth_month=%s WHERE id = %s', [birth_month,id])
	if birth_year != row[2]:
		cursor.execute('UPDATE person SET birth_year=%s WHERE id = %s', [birth_year,id])
	if death_day != row[3]:
		cursor.execute('UPDATE person SET death_day=%s WHERE id = %s', [death_day,id])
	if death_month != row[4]:
		cursor.execute('UPDATE person SET death_month=%s WHERE id = %s', [death_month,id])
	if death_year != row[5]:
		cursor.execute('UPDATE person SET death_year=%s WHERE id = %s', [death_year,id])
connection.commit()

cursor.execute('SELECT id, start_work_year, end_work_year FROM person')

for row in cursor.fetchall():
	id = row[0]
	start_work_year = row[1]
	end_work_year = row[2]

	if start_work_year is not None:
		try:
			datetime.date(start_work_year,1,1)
		except ValueError as e:
			start_work_year = None

	if end_work_year is not None:
		try: 
			datetime.date(end_work_year,1,1)
		except ValueError as e:
			end_work_year = None

	if start_work_year is not None and end_work_year is not None:
		if start_work_year > end_work_year:
			start_work_year = None
			end_work_year = None

	if start_work_year != row[1]:
		cursor.execute('UPDATE person SET start_work_year=%s WHERE id = %s', [start_work_year,id])
	if end_work_year != row[2]:
		cursor.execute('UPDATE person SET end_work_year=%s WHERE id = %s', [end_work_year,id])

connection.commit()

cursor.execute('SELECT id, release_day, release_month, release_year FROM movie')

for row in cursor.fetchall():
	id = row[0]
	release_day = row[1]
	release_month = row[2]
	release_year = row[3]

	release_date = parse_to_date_object(release_day,release_month,release_year)

cursor.execute('SELECT id, publication_day, publication_month, publication_year FROM work')

for row in cursor.fetchall():
	id = row[0]
	publication_day = row[1]
	publication_month = row[2]
	publication_year = row[3]

	try:
		publication_date = parse_to_date_object(publication_day,publication_month,publication_year)
	except ValueError as e:
		if str(e) == 'day is out of range for month':
			publication_day = None
			publication_date = parse_to_date_object(publication_day,publication_month,publication_year)
		elif str(e) == 'year is out of range':
			publication_year = None	
			publication_date = parse_to_date_object(publication_day,publication_month,publication_year)

	if publication_day != row[1]:
		cursor.execute('UPDATE work SET publication_day=%s WHERE id = %s', [publication_day,id])
	if publication_year != row[3]:
		cursor.execute('UPDATE work SET publication_year=%s WHERE id = %s', [publication_year,id])
connection.commit()








