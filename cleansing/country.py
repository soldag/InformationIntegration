from __future__ import division
import math
from os import dup
import psycopg2
import pycountry
from babel import Locale

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='Rosa',
                              database='infint_integrated')
cursor = connection.cursor()

#standardize data
cursor.execute('SELECT id,name FROM country')
for row in cursor.fetchall():
    country_id = row[0]
    name = row[1]
    if name is not None:
        if '\\' == name[0]:
            name = name[1:3]
            name = name.upper()
        try:
            country = pycountry.countries.get(alpha2=name)
            name = country.name
        except:
            if name == 'AN':
                name = 'Andorra'
            elif name == 'BU':
                name = 'Bulgaria'
            elif name == 'CT':
                name = 'Central African Republic'
            elif name == 'DA':
                name = 'Denmark'
            elif name == 'DU':
                name = 'Holland'
            elif name == 'IC':
                name = 'Iceland'
            elif name == 'JA':
                name = 'Japan'
            elif name == 'KO':
                name = 'South Korea'
            elif name == 'OS':
                name = 'Austria'
            elif name == 'PO':
                name = 'Portugal'
            elif name == 'RM':
                name = 'Romania'
            elif name == 'SP':
                name = 'Spain'
            elif name == 'SW':
                name = 'Sweden'
            elif name == 'TA':
                name = 'Tajikistan'
            elif name == 'TU':
                name = 'Turkey'
            elif name == 'UN':
                name = 'Ukraine'
            elif name == 'WA':
                name = 'Namibia'
            elif name == 'WI':
                name = 'Western Sahara'
            elif name == 'YU':
                name = 'Yugoslavia'
        cursor.execute('UPDATE country SET name=%s WHERE id = %s', [name,country_id])
connection.commit()

#duplicate detection
cursor.execute('SELECT DISTINCT c1.name FROM country AS c1, country AS c2 WHERE c1.id > c2.id AND c1.name = c2.name ORDER BY c1.name')
for row in cursor.fetchall():
    duplicate_country_name = row[0]
    cursor.execute('SELECT id, name FROM country WHERE name = %s', [duplicate_country_name])
    country_id = cursor.fetchone()[0]
    for element in cursor.fetchall():
        duplicate_id = element[0]
        #change entries in movie_country table
        cursor.execute('SELECT movie_id, type FROM movie_country WHERE country_id = %s', [duplicate_id])
        for join_tuple in cursor.fetchall():
            movie_id = join_tuple[0]
            type = join_tuple[1]
            cursor.execute('SELECT * FROM movie_country WHERE country_id = %s AND movie_id = %s AND type = %s', [country_id,movie_id,type])
            existing_tuple = cursor.fetchone()
            if existing_tuple is None:
                cursor.execute('UPDATE movie_country SET country_id=%s WHERE country_id = %s AND movie_id = %s AND type = %s', [country_id,duplicate_id,movie_id,type])
            else:
                cursor.execute('DELETE FROM movie_country WHERE country_id=%s AND movie_id = %s AND type = %s', [duplicate_id,movie_id,type])

        #change origin_country_id in table person
        cursor.execute('UPDATE person SET origin_country_id=%s WHERE origin_country_id = %s', [country_id,duplicate_id])
        
        #delete tuple from country table
        cursor.execute('DELETE FROM country WHERE id = %s', [duplicate_id])
connection.commit()


cursor.execute('SELECT id,name FROM country WHERE name LIKE \'R:%\' OR name LIKE \'[1]\' OR name LIKE \'action hero\' OR name LIKE \'cantankerous\' OR name LIKE \'comedian\' OR name LIKE \'orig\' OR name LIKE \'teenager\' OR name LIKE \'|\' OR name LIKE \'~1941\'')
for row in cursor.fetchall():
    country_id = row[0]
    name = row[1]
    #delete entries in movie_country table
    cursor.execute('DELETE FROM  movie_country WHERE country_id = %s', [country_id])
    #change origin_country_id in table person
    cursor.execute('UPDATE person SET origin_country_id=%s WHERE origin_country_id = %s', [None,country_id])
    #delete from country table
    cursor.execute('DELETE FROM country WHERE id = %s', [country_id])
connection.commit()
