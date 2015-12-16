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
									database='infint_integrated_final',
									user='Rosa')

cursor = connection.cursor()

cursor.execute('SELECT birth_day, birth_month, birth_year, death_day, death_month, death_year, id FROM person')

i = 0
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
		print e
		if str(e) == 'day is out of range for month':
			birth_day = None
			birth_date = parse_to_date_object(birth_day,birth_month,birth_year)
		elif str(e) == 'month must be in 1..12':
			birth_month = None
			birth_date = parse_to_date_object(birth_day,birth_month,birth_year)
	try:
		death_date = parse_to_date_object(death_day,death_month,death_year)
	except ValueError as e:
		print e
		if str(e) == 'day is out of range for month':
			death_day = None
			death_date = parse_to_date_object(death_day,death_month,death_year)
		elif str(e) == 'month must be in 1..12':
			death_month = None
			death_date = parse_to_date_object(death_day,death_month,death_year)
	if birth_date is not None and death_date is not None:
		if birth_date > death_date:
			i += 1
			birth_day = None
			birth_month = None
			birth_year = None
			death_day = None
			death_month = None
			death_year = None
	if birth_day != row[0]:
		cursor.execute('UPDATE person SET birth_day=%s WHERE id = %s', [birth_day,id])
	elif birth_month != row[1]:
		cursor.execute('UPDATE person SET birth_month=%s WHERE id = %s', [birth_month,id])
	elif birth_year != row[2]:
		cursor.execute('UPDATE person SET birth_year=%s WHERE id = %s', [birth_year,id])
	elif death_day != row[3]:
		cursor.execute('UPDATE person SET death_day=%s WHERE id = %s', [death_day,id])
	elif death_month != row[4]:
		cursor.execute('UPDATE person SET death_month=%s WHERE id = %s', [death_month,id])
	elif death_year != row[5]:
		cursor.execute('UPDATE person SET death_year=%s WHERE id = %s', [death_year,id])
connection.commit()







