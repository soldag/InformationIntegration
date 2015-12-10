import re
import psycopg2


class UciKddMapper:
    ID_PREFIX = 'uci_'
    JOB_ACTOR = 1
    JOB_PRODUCER = 2
    JOB_DIRECTOR = 3
    DEFAULT_ROLE = ''

    def __init__(self, host, port, user, password, src_database, dst_database):
        self.src_connection, self.src_cursor = self._connect(host, port, user, password, src_database)
        self.dst_connection, self.dst_cursor = self._connect(host, port, user, password, dst_database)

        self.last_ids = {}
        self.retrieve_last_ids()

    def _connect(self, host, port, user, password, database):
        connection = psycopg2.connect(host=host,
                                      port=port,
                                      user=user,
                                      password=password,
                                      database=database)
        cursor = connection.cursor()

        return connection, cursor

    def retrieve_last_ids(self):
        for table in ['trailer', 'rating', 'country', 'location']:
            self.dst_cursor.execute('SELECT MAX(id) FROM {}'.format(table))
            self.last_ids[table] = self.dst_cursor.fetchone()[0] or 0

    def map(self):
        print "Integrate actors table..."
        self.src_cursor.execute('SELECT * FROM actors')
        for row in self.src_cursor.fetchall():
            country_id = self.get_or_create_country(row[8])
            birth_year = self.parse_int(row[5])
            death_year = self.parse_int(row[6])
            start_work_year, end_work_year = self.split_years(row[1])
            self.create_or_update_person(row[0], row[3], row[2], row[4], birth_year, death_year, row[0],
                                         start_work_year, end_work_year, row[9], country_id, row[10])

        print "Integrate people table..."
        self.src_cursor.execute('SELECT * FROM people')
        for row in self.src_cursor.fetchall():
            birth_year = self.parse_int(row[6])
            death_year = self.parse_int(row[7])
            start_work_year, end_work_year = self.split_years(row[3])
            self.create_or_update_person(row[0], row[5], row[4], birth_year=birth_year, death_year=death_year,
                                         start_work_year=start_work_year, end_work_year=end_work_year, comment=row[8])

        print "Integrate movie table..."
        self.src_cursor.execute('SELECT * FROM movies')
        for row in self.src_cursor.fetchall():
            title = row[1]
            if title.startswith('T:'):
                title = title[2:]
            release_year = self.parse_int(row[2])
            movie_id = self.create_movie(row[0], title, release_year, row[5], row[6], row[7], row[10])

            if movie_id:
                if row[3]:
                    person_id = self.ID_PREFIX + row[3]
                    self.create_person_movie_ref(person_id, movie_id, self.JOB_DIRECTOR, self.DEFAULT_ROLE)
                if row[4]:
                    person_id = self.ID_PREFIX + row[4]
                    self.create_person_movie_ref(person_id, movie_id, self.JOB_PRODUCER, self.DEFAULT_ROLE)
                if row[9]:
                    locations = row[9].split(';')
                    for location in locations:
                        location = location.strip()
                        location_id = self.get_or_create_location(location)
                        self.create_movie_location_ref(movie_id, location_id)

        print "Integrate casts table..."
        self.src_cursor.execute('SELECT * FROM casts')
        for row in self.src_cursor.fetchall():
            movie_id = self.ID_PREFIX + row[0]
            person_id = self.ID_PREFIX + row[3]
            self.create_person_movie_ref(person_id, movie_id, self.JOB_ACTOR, row[5], row[4], row[6], row[7])

    def parse_int(self, value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def split_years(self, span):
        if isinstance(span, str) or isinstance(span, unicode):
            span = span.replace('@', '')
            split_dow = span.split('-')
            if len(split_dow) > 0:
                start_year = self.parse_int(split_dow[0])
                end_year = None
                if len(split_dow) > 1:
                    end_year = self.parse_int(split_dow[1])
                return start_year, end_year

        return None, None

    def get_or_create_country(self, country_name):
        if country_name:
            self.dst_cursor.execute('SELECT id FROM country WHERE name = %s', [country_name])
            row = self.dst_cursor.fetchone()
            if row is not None:
                country_id = row[0]
            else:
                self.last_ids['country'] += 1
                country_id = self.last_ids['country']
                self.dst_cursor.execute('INSERT INTO country VALUES(%s,%s)', [country_id, country_name])
                self.dst_connection.commit()

            return country_id

    def get_or_create_location(self, location_name):
        if location_name:
            self.dst_cursor.execute('SELECT id FROM location WHERE name = %s', [location_name])
            row = self.dst_cursor.fetchone()
            if row is not None:
                location_id = row[0]
            else:
                self.last_ids['location'] += 1
                location_id = self.last_ids['location']
                self.dst_cursor.execute('INSERT INTO location VALUES(%s,%s)', [location_id, location_name])
                self.dst_connection.commit()

            return location_id

    def create_or_update_person(self, id, first_name, last_name, gender=None, birth_year=None, death_year=None,
                                stage_name=None, start_work_year=None, end_work_year=None,
                                photo=None, origin_country_id=None, comment=None):
        person_id = self.ID_PREFIX + id
        self.dst_cursor.execute('SELECT id FROM person WHERE id = %s', [person_id])
        if self.dst_cursor.fetchone() is not None:
            try:
                self.dst_cursor.execute('UPDATE person SET first_name=%s,last_name=%s,gender=%s,birth_year=%s,death_year=%s,stage_name=%s,start_work_year=%s,end_work_year=%s,photo=%s,origin_country_id=%s,comment=%s WHERE id = %s',
                                        [first_name, last_name, gender, birth_year, death_year, stage_name,
                                         start_work_year, end_work_year, photo, origin_country_id, comment, person_id])
                self.dst_connection.commit()
            except psycopg2.DataError:
                self.dst_connection.rollback()
        else:
            try:
                self.dst_cursor.execute('INSERT INTO person (id,first_name,last_name,gender,birth_year,death_year,stage_name,start_work_year,end_work_year,photo,origin_country_id,comment) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                        [person_id, first_name, last_name, gender, birth_year, death_year, stage_name,
                                         start_work_year, end_work_year, photo, origin_country_id, comment])
                self.dst_connection.commit()
            except psycopg2.DataError:
                self.dst_connection.rollback()
                return None

        return person_id

    def create_movie(self, id, title, release_year, studios, prc, genre, comment):
        movie_id = self.ID_PREFIX + id
        try:
            self.dst_cursor.execute('INSERT INTO movie (id,title,release_year,studios,process_used_to_produce,genre,comment) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                    [movie_id, title, release_year, studios, prc, genre, comment])
            self.dst_connection.commit()
        except psycopg2.DataError:
            self.dst_connection.rollback()
            return None

        return movie_id

    def create_person_movie_ref(self, person_id, movie_id, job_id, role, role_type=None, awards=None, comment=None):
        if role is None:
            role = self.DEFAULT_ROLE

        self.dst_cursor.execute('SELECT * FROM person_movie WHERE person_id=%s AND movie_id=%s AND job_id=%s AND role=%s',
                                [person_id, movie_id, str(job_id), role])
        if self.dst_cursor.fetchone() is None:
            self.dst_cursor.execute('INSERT INTO person_movie (person_id,movie_id,job_id,role,role_type,awards,comment) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                    [person_id, movie_id, job_id, role, role_type, awards, comment])
            self.dst_connection.commit()

    def create_movie_location_ref(self, movie_id, location_id):
        self.dst_cursor.execute('INSERT INTO movie_location (movie_id,location_id) VALUES(%s,%s)',
                                [movie_id, location_id])
        self.dst_connection.commit()

    def close(self):
        self.src_connection.close()
        self.dst_connection.close()
