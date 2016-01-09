import psycopg2

connection = psycopg2.connect(host='localhost',
							  port=5432,
							  user='Henni',
							  database='integrated_table')
cursor = connection.cursor()


cursor.execute('Select id FROM work WHERE (title IS NULL) AND (edition_information IS NULL) AND (format IS NULL) AND (description IS NULL) AND (isbn IS NULL) AND (isbn13 IS NULL) AND (number_of_pages IS NULL) AND (publication_day IS NULL) AND (publication_month IS NULL) AND (publication_year IS NULL) AND (publisher IS NULL) AND (text_reviews_count IS NULL) AND (rating_ID IS NULL)')
for row in cursor.fetchall():
	id = row[0]

	#delete tuple from work_person table
	cursor.execute('DELETE FROM work_person WHERE work_id=%s', [id])

	#delete tuple from work table
	cursor.execute('DELETE FROM work WHERE id = %s', [id])

connection.commit()




