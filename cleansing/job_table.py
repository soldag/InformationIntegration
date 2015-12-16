import psycopg2

connection = psycopg2.connect(host='localhost',
									   port=5432,
									   database='integrated_table',
									   user='Henni')

cursor = connection.cursor()

cursor.execute('SELECT name FROM job')
for row in cursor.fetchall():
	name = row[0]
	print name
	if name is not None:
		if name[0].islower():
			name[0].upper
			print name
