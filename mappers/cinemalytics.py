import re
import psycopg2


class CinemalyticsMapper:
    ID_PREFIX = 'cm_'
    DEFAULT_COUNTRY_TYPE = 'production'
    DEFAULT_JOB_ID = 1
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
        for table in ['trailer', 'rating', 'country']:
            self.dst_cursor.execute('SELECT MAX(id) FROM {}'.format(table))
            self.last_ids[table] = self.dst_cursor.fetchone()[0] or 0

    def map(self):
        print "Integrate movie table..."
        self.src_cursor.execute('SELECT * FROM cinemalytics.movies')
        for row in self.src_cursor.fetchall():
            release_day, release_month, release_year = self.parse_date(row[13])
            trailer_id = self.create_trailer(row[5], row[6])
            rating_id = self.create_rating(row[11], row[10])
            movie_id = self.create_movie(row[0], row[1], row[3], row[2], row[4], trailer_id, row[8], row[9], row[12],
                                         release_day, release_month, release_year, row[14], row[15], row[16], row[17],
                                         rating_id)

            country_id = self.get_or_create_country(row[7])
            self.create_movie_country_ref(movie_id, country_id)
        self.dst_connection.commit()

        print "Integrate actors table..."
        self.src_cursor.execute('SELECT * FROM cinemalytics.actors')
        for row in self.src_cursor.fetchall():
            first_name, last_name = self.split_name(row[1])
            rating_id = self.create_rating(row[4])
            self.create_person(row[0], first_name, last_name, row[2], rating_id, row[3])
        self.dst_connection.commit()

        print "Integrate actor_movies table..."
        self.src_cursor.execute('SELECT * FROM cinemalytics.actor_movies')
        for row in self.src_cursor.fetchall():
            person_id = self.ID_PREFIX + row[0]
            movie_id = self.ID_PREFIX + row[1]
            self.create_person_movie_ref(person_id, movie_id)
        self.dst_connection.commit()

    def parse_date(self, date):
        day = month = year = None
        date_search = re.search('((?P<month>\d?\d)/(?P<day>\d?\d)/)?(?P<year>\d\d\d\d)', date)
        search_groups = date_search.groupdict()
        if 'day' in search_groups and 'month' in search_groups:
            day = search_groups['day']
            month = search_groups['month']
        if 'year' in search_groups:
            year = search_groups['year']

        return day, month, year

    def split_name(self, name):
        split_names = name.split(' ')
        first_name = ' '.join(split_names[:-1])
        last_name = split_names[-1]

        return first_name, last_name

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

    def create_rating(self, average, count=None):
        self.last_ids['rating'] += 1
        rating_id = self.last_ids['rating']
        self.dst_cursor.execute('INSERT INTO rating VALUES(%s,%s,%s)', [rating_id, average, count])
        self.dst_connection.commit()

        return rating_id

    def create_trailer(self, link, embed_code):
        if link or embed_code:
            self.last_ids['trailer'] += 1
            trailer_id = self.last_ids['trailer']
            self.dst_cursor.execute('INSERT INTO trailer VALUES(%s,%s,%s)', [trailer_id, link, embed_code])
            self.dst_connection.commit()

            return trailer_id

    def create_movie(self, id, imdb_id, title, original_title, description, trailer_id, region,
                     genre, censor_rating, release_day, release_month, release_year,
                     runtime, budget, revenue, poster_path, rating_id):
        movie_id = self.ID_PREFIX + id
        self.dst_cursor.execute('INSERT INTO movie (id,imdb_id,title,original_title,description,trailer_id,region,genre,censor_rating,release_day,release_month,release_year,runtime,budget,revenue,poster_path,rating_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                [movie_id, imdb_id, title, original_title, description, trailer_id,
                                 region, genre, censor_rating, release_day, release_month, release_year,
                                 runtime, budget, revenue, poster_path, rating_id])
        self.dst_connection.commit()

        return movie_id

    def create_person(self, id, first_name, last_name, gender, rating_id, profile_photo_path):
        person_id = self.ID_PREFIX + id
        self.dst_cursor.execute('INSERT INTO person (id,first_name,last_name,gender,rating_id,profile_photo_path) VALUES(%s,%s,%s,%s,%s,%s)',
                                [person_id, first_name, last_name, gender, rating_id, profile_photo_path])
        self.dst_connection.commit()

        return person_id

    def create_movie_country_ref(self, movie_id, country_id):
        self.dst_cursor.execute('INSERT INTO movie_country VALUES(%s,%s,%s)', [country_id, movie_id, self.DEFAULT_COUNTRY_TYPE])
        self.dst_connection.commit()

    def create_person_movie_ref(self, person_id, movie_id):
        self.dst_cursor.execute('INSERT INTO person_movie (person_id,movie_id,job_id,role) VALUES(%s,%s,%s,%s)',
                                [person_id, movie_id, self.DEFAULT_JOB_ID, self.DEFAULT_ROLE])
        self.dst_connection.commit()

    def close(self):
        self.src_connection.close()
        self.dst_connection.close()
