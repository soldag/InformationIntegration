import psycopg2

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='Rosa',
                              database='infint_integrated')
cursor = connection.cursor()

cursor.execute('UPDATE movie SET poster_path=NULL WHERE poster_path = \'https://s3-ap-southeast-1.amazonaws.com/cinemalytics/movie/\'')
connection.commit()