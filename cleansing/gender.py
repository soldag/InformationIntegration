from __future__ import division
import math
import psycopg2

MALE = 'm'
FEMALE = 'f'

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='soldag',
                              password='',
                              database='integrated')
cursor = connection.cursor()

cursor.execute('SELECT COUNT(id) FROM person WHERE gender NOT IN (%s,%s)', [MALE, FEMALE])
person_count = cursor.fetchone()[0]

i = 0
lastProcess = -1
cursor.execute('SELECT id,gender FROM person WHERE gender NOT IN (%s,%s)', [MALE, FEMALE])
for row in cursor.fetchall():
    person_id = row[0]
    gender = row[1]
    if gender:
        if gender.lower() == MALE:
            gender = MALE
        elif gender.lower() == FEMALE:
            gender = FEMALE
        else:
            gender = None
    else:
        gender = None
    if row[1] != gender:
        cursor.execute('UPDATE person SET gender=%s WHERE id=%s', [gender, person_id])

    i += 1
    process = i / person_count * 100
    if lastProcess == -1 or process - lastProcess >= 1:
        print "%d%% completed" % int(math.floor(process))
        lastProcess = process

connection.commit()
