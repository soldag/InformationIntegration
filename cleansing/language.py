from __future__ import division
import math
from os import dup
import psycopg2
from babel import Locale

connection = psycopg2.connect(host='localhost',
							  port=5432,
							  user='Rosa',
							  database='infint_integrated')
cursor = connection.cursor()

#standardize and clean data 
cursor.execute('SELECT id,language FROM language')
for row in cursor.fetchall():
	language_id = row[0]
	language = row[1]
	try:
		locale = Locale.parse(language)
		language = locale.get_language_name('en_US')
	except:
		if language == 'la':
			language = 'Latin'
		elif language == 'cn':
			language = 'Chinese (simplified)'
		elif language == 'xx':
			language = None
		elif language == 'nv':
			language = 'Navajo'
		elif language == 'sh':
			language = 'Serbo-Croatian'
		elif language == 'yi':
			language = 'Yiddish'
		elif language == 'mi':
			language = 'Maori'
		elif language == 'tt':
			language = 'Tatar'
		elif language == 'ht':
			language = 'Haitian Creole'
		elif language == 'qu':
			language = 'Quechua'
		elif language == 'sa':
			language = 'Sanskrit'
		elif language == 'co':
			language = 'Corsican'
		elif language == 'sc':
			language = 'Sardinian'
		elif language == 'ku':
			language = 'Kurdish'
		elif language == 'ce':
			language = 'Chechen'
		elif language == 'ay':
			language = 'Aymara'
		elif language == 'lb':
			language = 'Luxembourgish'
		elif language == 'iu':
			language = 'Inuktitut'
		elif language == 'gn':
			language = 'Guarani'
		elif language == 'ty':
			language = 'Tahitian'
		elif language == 'oc':
			language = 'Occitan'
		elif language == 'ny':
			language = 'Nyanja'
		elif language == 'jv':
			language = 'Javanese'
		elif language == 'wo':
			language = 'Wolof'
		elif language == 'sm':
			language = 'Samoan'
		elif language == 'cr':
			language = 'Cree'
		elif language == 'ab':
			language = 'Abkhazian'
		elif language == 'fy':
			language = 'Frisian'
		elif language == 'tk':
			language = 'Turkmen'
		elif language == 'su':
			language = 'Sundanese'
		elif language == 'cu':
			language = 'Old Church Slavonic'
		elif language == 'mh':
			language = 'Marshallese language'
		elif language == 'za':
			language = 'English (South Africa)'
		
	cursor.execute('UPDATE language SET language=%s WHERE id = %s', [language,language_id])
connection.commit()

#Delete duplicates
cursor.execute('SELECT DISTINCT L1.language FROM language AS L1, language AS L2 WHERE L1.id != L2.id AND L1.language = L2.language')
for row in cursor.fetchall():
	duplicate_language = row[0]
	cursor.execute('SELECT id, language FROM language WHERE language = %s', [duplicate_language])
	language_id = cursor.fetchone()[0]
	for element in cursor.fetchall():
		duplicate_id = element[0]
		#change entries in movie_language table
		cursor.execute('SELECT movie_id, type FROM movie_language WHERE language_id = %s', [duplicate_id])
		for join_tuple in cursor.fetchall():
			movie_id = join_tuple[0]
			type = join_tuple[1]
			cursor.execute('SELECT * FROM movie_language WHERE language_id = %s AND movie_id = %s AND type = %s', [language_id,movie_id,type])
			existing_tuple = cursor.fetchone()
			if existing_tuple is None:
				cursor.execute('UPDATE movie_language SET language_id=%s WHERE language_id = %s AND movie_id = %s AND type = %s', [language_id,duplicate_id,movie_id,type])
			else:
				cursor.execute('DELETE FROM movie_language WHERE language_id=%s AND movie_id = %s AND type = %s', [duplicate_id,movie_id,type])

		#delete tuple from langaguage table
		cursor.execute('DELETE FROM language WHERE id = %s', [duplicate_id])

connection.commit()




