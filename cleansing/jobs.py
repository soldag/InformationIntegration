from __future__ import division
import math
from os import dup
import psycopg2

connection = psycopg2.connect(host='localhost',
                              port=5432,
                              user='soldag',
                              password='',
                              database='integrated')
cursor = connection.cursor()

# Delete jobs that are not referenced
print "Delete abandoned jobs..."
cursor.execute('DELETE FROM job WHERE id NOT IN (SELECT DISTINCT(job_id) FROM person_movie)')

# Standardize & clean remaining jobs
print "Standardize & clean jobs..."
cursor.execute('SELECT COUNT(id) FROM job')
job_count = cursor.fetchone()[0]

i = 0
lastProcess = -1
processed_jobs = []
cursor.execute('SELECT id,name FROM job')
for row in cursor.fetchall():
    job_id = row[0]
    name = row[1]

    # Check, if job has already been processed
    if job_id in processed_jobs:
        continue

    # Standardize job names
    if name.startswith('x_'):
        name = name[2:]
    if not name.istitle():
        name = name.title()

    # Remove duplicates
    cursor.execute('SELECT id FROM job WHERE id !=%s AND name=%s', [job_id, name])
    duplicate_job_ids = tuple(x[0] for x in cursor.fetchall())
    if duplicate_job_ids:
        # Adapt references in person_movie
        # Delete duplicates with the same primary key attribute values (except job_id)
        cursor.execute('DELETE FROM person_movie AS P1 USING person_movie AS P2 '
                       'WHERE P1.person_id=P2.person_id AND P1.movie_id=P2.movie_id AND P1.role=P2.role '
                       'AND P1.job_id IN %s AND P2.job_id=%s', [duplicate_job_ids, job_id])

        # Update remaining duplicates
        cursor.execute('UPDATE person_movie SET job_id=%s WHERE job_id IN %s', [job_id, duplicate_job_ids])

        # Remove duplicate job entries
        cursor.execute('DELETE FROM job WHERE id IN %s', [duplicate_job_ids])

        connection.commit()

    # Update name, if necessary
    if name != row[1]:
        cursor.execute('UPDATE job SET name=%s WHERE id=%s', [name, job_id])
        connection.commit()

    # Update processed jobs list
    processed_jobs.append(job_id)
    processed_jobs += duplicate_job_ids

    # Print progress
    i += 1 + len(duplicate_job_ids)
    process = i / job_count * 100
    if lastProcess == -1 or process - lastProcess >= 1:
        print "%d%% completed" % int(math.floor(process))
        lastProcess = process
