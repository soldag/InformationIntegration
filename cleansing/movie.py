from __future__ import division
import re
import math
import operator
import psycopg2
from Levenshtein.StringMatcher import StringMatcher


SIMILARITY_THRESHOLD = 0.95


def clean_movie():
    connection = psycopg2.connect(host='localhost',
                                  port=5432,
                                  user='Henni',
                                  database='integrated_table')

    # Get database cursors
    select_cursor = connection.cursor()
    edit_cursor = connection.cursor()

    print "Apply blocking 1"
    duplicates_count = split_into_blocks(select_cursor, edit_cursor,
                                         'SELECT SUBSTR(LOWER(title), 1, 8) AS title_1, SUBSTR(LOWER(original_title), 1, 4) AS original_title_1 FROM movie WHERE LOWER(title) LIKE %s GROUP BY title_1, original_title_1',
                                         ['the %'])

    print "Apply blocking 2"
    duplicates_count += split_into_blocks(select_cursor, edit_cursor,
                                          'SELECT SUBSTR(LOWER(title), 1, 8) AS title_1, SUBSTR(LOWER(original_title), 1, 4) AS original_title_1 FROM movie WHERE LOWER(title) LIKE %s GROUP BY title_1, original_title_1',
                                          ['die %'])

    print "Apply blocking 3"
    duplicates_count += split_into_blocks(select_cursor, edit_cursor,
                                          'SELECT SUBSTR(LOWER(title), 1, 8) AS title_1, SUBSTR(LOWER(original_title), 1, 4) AS original_title_1 FROM movie WHERE LOWER(title) NOT LIKE %s AND LOWER(title) NOT LIKE %s GROUP BY title_1, original_title_1',
                                          ['the %', 'die %'])

    # Commit all database changes
    connection.commit()

    print('%d duplicates found.' % duplicates_count)


def split_into_blocks(select_cursor, edit_cursor, query, arguments=None):
    if arguments is None:
        arguments = []

    i = 0
    last_process = 0
    duplicates_count = 0
    select_cursor.execute(query, arguments)
    groups = select_cursor.fetchall()
    for group in groups:
        if group[0] is None:
            select_cursor.execute('SELECT * FROM movie WHERE title IS NULL AND original_title LIKE %s', [group[1]+'%'])
        elif group[1] is None:
            select_cursor.execute('SELECT * FROM movie WHERE title LIKE %s AND original_title IS NULL', [group[0]+'%'])
        else:
            select_cursor.execute('SELECT * FROM movie WHERE title LIKE %s AND original_title LIKE %s', [group[0]+'%', group[1]+'%'])
        row_bucket = select_cursor.fetchall()
        duplicates_count += find_duplicates(edit_cursor, row_bucket)

        # Calculate and print progress
        i += 1
        process = i / len(groups) * 100
        if last_process == -1 or process - last_process >= 1:
            print "%d%% completed" % int(math.floor(process))
            last_process = process

    return duplicates_count


def find_duplicates(cursor, rows):
    checked_row_ids = []
    duplicates_count = 0
    for i in range(0, len(rows)):
        row1 = rows[i]
        if row1 and row1[0] not in checked_row_ids:
            duplicate_rows = [row1]
            for j in range(i + 1, len(rows)):
                row2 = rows[j]
                if row2 and row2[0] not in checked_row_ids:
                    similarity = calculate_similarity(row1, row2)
                    if similarity >= SIMILARITY_THRESHOLD:
                        duplicate_rows.append(row2)
                        checked_row_ids.extend([row1[0], row2[0]])
            if len(duplicate_rows) > 1:
                merge_duplicates(cursor, duplicate_rows)
                duplicates_count += len(duplicate_rows)

    return duplicates_count


def calculate_similarity(row1, row2):
    title_sim = title_similarity(row1[2], row2[2])
    original_title_sim = title_similarity(row1[3], row2[3])
    release_year_sim = year_sim(row1[11], row2[11])
    imdb_id_sim = imdb_id_similarity(row1[1], row2[1])

    return 0.4 * imdb_id_sim + 0.3 * title_sim + 0.2 * release_year_sim + 0.2 * original_title_sim


def title_similarity(title1, title2):
    if any([x is None for x in [title1, title2]]):
        return 1

    return normalized_levenshtein(title1, title2)


def imdb_id_similarity(imdb_id_1, imdb_id_2):
    if any([x is None for x in [imdb_id_1, imdb_id_2]]):
        return 1

    if imdb_id_1 == imdb_id_2:
        return 1

    return 0


def year_sim(year1, year2):
    if any([x is None for x in [year1, year2]]):
        return 1

    sim = year1 - year2

    if sim == 0:
        return 1
    elif -3 < sim < 3:
        return 0.75
    else:
        return 0


def normalized_levenshtein(string1, string2):
    distance = levenshtein(string1, string2)
    if distance == 0:
        return 1
    else:
        return 1 - distance / max_length(string1, string2)


def levenshtein(string1, string2):
    if string1 is None:
        string1 = ""
    if string2 is None:
        string2 = ""

    string_matcher = StringMatcher(seq1=string1.lower(), seq2=string2.lower())
    return string_matcher.distance()


def max_length(string1, string2):
    if string1 is None:
        return len(string2)
    if string2 is None:
        return len(string1)

    return max(len(string1), len(string2))


def merge_duplicates(cursor, duplicate_rows):
    taken_values_count = dict(enumerate([0]*len(duplicate_rows)))

    # Merge values
    movie_id = duplicate_rows[0][0]
    title, title_index = get_longest_string(duplicate_rows, 2, taken_values_count)
    imdb_id = duplicate_rows[awesome_function(duplicate_rows, 1, taken_values_count)][1]
    original_title = duplicate_rows[title_index][3]
    description = get_longest_string(duplicate_rows, 4, taken_values_count)
    trailer_id = duplicate_rows[awesome_function(duplicate_rows, 5, taken_values_count)][5]
    region = duplicate_rows[awesome_function(duplicate_rows, 6, taken_values_count)][6]
    censor_rating = get_censor_rating(duplicate_rows, 7, taken_values_count)
    day, month, year = get_date(duplicate_rows, taken_values_count)
    runtime = duplicate_rows[awesome_function(duplicate_rows, 11, taken_values_count)][11]
    budget = duplicate_rows[awesome_function(duplicate_rows, 12, taken_values_count)][12]
    revenue = duplicate_rows[awesome_function(duplicate_rows, 13, taken_values_count)][13]
    poster_path = get_longest_string(duplicate_rows, 14, taken_values_count)
    homepage = duplicate_rows[awesome_function(duplicate_rows, 15, taken_values_count)][15]
    popularity = get_largest_number(duplicate_rows, 16, taken_values_count)
    status = get_status(duplicate_rows, 17, taken_values_count)
    tagline = get_longest_string(duplicate_rows, 18, taken_values_count)
    video = get_video(duplicate_rows, 19, taken_values_count)
    rating_id = get_rating_id(duplicate_rows, 20, cursor)
    parent_id = get_longest_string(duplicate_rows, 21, taken_values_count)
    studios = duplicate_rows[awesome_function(duplicate_rows, 22, taken_values_count)][22]
    process_used_to_produce = concat_attributes(duplicate_rows, 23)
    comment = concat_attributes(duplicate_rows, 24)

    # Update values of first duplicate row
    cursor.execute('UPDATE movie SET imdb_id = %s, title = %s, original_title = %s, description = %s, trailer_id = %s,' 
                        'region = %s, censor_rating = %s, release_day  = %s, release_month  = %s, release_year  = %s,'
                        ' runtime = %s, budget = %s, revenue = %s, poster_path = %s, homepage = %s, popularity = %s, status = %s, '
                        'tagline = %s, video = %s, rating_id  = %s, parent_id = %s, studios = %s, process_used_to_produce = %s, comment = %s WHERE id=%s',
                   [
                       imdb_id, title, original_title, description, trailer_id, region,
                       censor_rating, day, month, year, runtime, budget, revenue, poster_path, homepage, popularity, status, tagline, video, rating_id, parent_id, studios,
                       process_used_to_produce, comment, movie_id
                   ])

    # Remove other duplicate rows
    dup_movie_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != movie_id])
    for dup_movie_id in dup_movie_ids:
        cursor.execute('SELECT person_id, job_id, role FROM person_movie WHERE movie_id=%s', [dup_movie_id])
        for row in cursor.fetchall():
            person_id = row[0]
            job_id = row[1]
            role = row[2]
            cursor.execute('SELECT * FROM person_movie WHERE movie_id=%s AND person_id=%s AND job_id=%s AND role=%s', [movie_id, person_id, job_id, role])
            if cursor.fetchone():
                cursor.execute('DELETE FROM person_movie WHERE movie_id=%s AND person_id=%s AND job_id=%s AND role=%s', [dup_movie_id, person_id, job_id, role])
            else:
                cursor.execute('UPDATE person_movie SET movie_id=%s WHERE movie_id=%s AND person_id=%s AND job_id=%s AND role=%s',
                               [movie_id, dup_movie_id, person_id, job_id, role])

    dup_movie_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != movie_id])
    for dup_movie_id in dup_movie_ids:
        cursor.execute('SELECT language_id, type FROM movie_language WHERE movie_id=%s', [dup_movie_id])
        for row in cursor.fetchall():
            language_id = row[0]
            type = row[1]
            cursor.execute('SELECT * FROM movie_language WHERE movie_id=%s AND language_id=%s AND type=%s', [movie_id, language_id, type])
            if cursor.fetchone():
                cursor.execute('DELETE FROM movie_language WHERE movie_id=%s AND language_id=%s AND type=%s', [dup_movie_id, language_id, type])
            else:
                cursor.execute('UPDATE movie_language SET movie_id=%s WHERE movie_id=%s AND language_id=%s AND type=%s',
                               [movie_id, dup_movie_id, language_id, type])

    dup_movie_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != movie_id])
    for dup_movie_id in dup_movie_ids:
        cursor.execute('SELECT country_id, type FROM movie_country WHERE movie_id=%s', [dup_movie_id])
        for row in cursor.fetchall():
            country_id = row[0]
            type = row[1]
            cursor.execute('SELECT * FROM movie_country WHERE movie_id=%s AND country_id=%s AND type=%s', [movie_id, country_id, type])
            if cursor.fetchone():
                cursor.execute('DELETE FROM movie_country WHERE movie_id=%s AND country_id=%s AND type=%s', [dup_movie_id, country_id, type])
            else:
                cursor.execute('UPDATE movie_country SET movie_id=%s WHERE movie_id=%s AND country_id=%s AND type=%s',
                               [movie_id, dup_movie_id, country_id, type])

    dup_movie_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != movie_id])
    for dup_movie_id in dup_movie_ids:
        cursor.execute('SELECT location_id FROM movie_location WHERE movie_id=%s', [dup_movie_id])
        for row in cursor.fetchall():
            location_id = row[0]
            cursor.execute('SELECT * FROM movie_location WHERE movie_id=%s AND location_id=%s', [movie_id, location_id])
            if cursor.fetchone():
                cursor.execute('DELETE FROM movie_location WHERE movie_id=%s AND location_id=%s', [dup_movie_id, location_id])
            else:
                cursor.execute('UPDATE movie_location SET movie_id=%s WHERE movie_id=%s AND location_id=%s',
                               [movie_id, dup_movie_id, location_id])

    dup_movie_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != movie_id])
    for dup_movie_id in dup_movie_ids:
        cursor.execute('SELECT genre_id FROM movie_genre WHERE movie_id=%s', [dup_movie_id])
        for row in cursor.fetchall():
            genre_id = row[0]
            cursor.execute('SELECT * FROM movie_genre WHERE movie_id=%s AND genre_id=%s', [movie_id, location_id])
            if cursor.fetchone():
                cursor.execute('DELETE FROM movie_genre WHERE movie_id=%s AND genre_id=%s', [dup_movie_id, location_id])
            else:
                cursor.execute('UPDATE movie_genre SET movie_id=%s WHERE movie_id=%s AND genre_id=%s',
                               [movie_id, dup_movie_id, genre_id])

    cursor.execute('DELETE FROM movie WHERE id IN %s', [dup_movie_ids])

    rating_ids = tuple([movie_id for movie_id in [row[20] for row in duplicate_rows] if movie_id is not None and movie_id != rating_id])
    if rating_ids:
        cursor.execute('DELETE FROM rating WHERE id IN %s', [rating_ids])

    trailer_ids = tuple([movie_id for movie_id in [row[5] for row in duplicate_rows] if movie_id is not None and movie_id != trailer_id])
    if trailer_ids:
        cursor.execute('DELETE FROM trailer WHERE id IN %s', [trailer_ids])


def get_longest_string(duplicate_rows, column_index, taken_values_count):
    strings = [x[column_index] for x in duplicate_rows]
    maximum_length = max(map(string_length, strings))
    candidate_indexes = [x for x in range(0, string_length(strings)) if string_length(strings[x]) == maximum_length]
    if len(candidate_indexes) == 1:
        index = candidate_indexes[0]
    else:
        index = get_most_used_row(candidate_indexes, taken_values_count)

    taken_values_count[index] += 1
    return strings[index], index


def get_largest_number(duplicate_rows, column_index, taken_values_count):
    numbers = [x[column_index] for x in duplicate_rows]
    index = numbers.index(max(numbers))

    taken_values_count[index] += 1
    return numbers[index]


def get_censor_rating(duplicate_rows, censor_rating_column_index, taken_values_count):
    ratings = [row[censor_rating_column_index] for row in duplicate_rows]
    if 'A' in ratings:
        index = ratings.indexOf('A')
    elif 'U/A' in ratings:
        index = ratings.indexOf('U/A')
    elif 'U' in ratings:
        index = ratings.indexOf('U')
    else:
        index = 0

    taken_values_count[index] += 1
    return ratings[index]


def get_status(duplicate_rows, status_column_index, taken_values_count):
    status = [row[status_column_index] for row in duplicate_rows]
    if 'Canceled' in status:
        index = status.indexOf('Canceled')
    elif 'Released' in status:
        index = status.indexOf('Released')
    elif 'Post Production' in status:
        index = status.indexOf('Post Production')
    elif 'In Production' in status:
        index = status.indexOf('In Production')
    elif 'Planned' in status:
        index = status.indexOf('Planned')
    elif 'Rumored' in status:
        index = status.indexOf('Rumored')
    else:
        index = 0

    taken_values_count[index] += 1
    return status[index]


def get_video(duplicate_rows, video_column_index, taken_values_count):
    videos = [row[video_column_index] for row in duplicate_rows]
    if True in videos:
        index = videos.indexOf(True)
    elif False in videos:
        index = videos.indexOf(False)
    else:
        index = 0

    taken_values_count[index] += 1
    return videos[index]


def concat_attributes(duplicate_rows, column_index):
    values = [row[column_index] for row in duplicate_rows]
    return '; '.join(values)



def get_date(duplicate_rows, taken_values_count):
    days = [x[8] for x in duplicate_rows]
    months = [x[9] for x in duplicate_rows]
    years = [x[10] for x in duplicate_rows]

    if all([x is None for x in years]):
        index = 0
    elif all([x is None for x in months]):
        candidate_indexes = [x for x in range(0, len(duplicate_rows))
                             if years[x] is not None]
        index = get_most_used_row(candidate_indexes, taken_values_count)
    elif all([x is None for x in days]):
        candidate_indexes = [x for x in range(0, len(duplicate_rows))
                             if years[x] is not None and months[x] is not None]
        index = get_most_used_row(candidate_indexes, taken_values_count)
    else:
        candidate_indexes = [x for x in range(0, len(duplicate_rows))
                             if years[x] is not None and months[x] is not None and days[x] is not None]
        index = get_most_used_row(candidate_indexes, taken_values_count)

    taken_values_count[index] += 1
    return duplicate_rows[index][8], duplicate_rows[index][9], duplicate_rows[index][10]


def get_rating_id(duplicate_rows, column_index, cursor):
    rating_ids = [x[column_index] for x in duplicate_rows if x[column_index] is not None]
    if rating_ids:
        cursor.execute('SELECT id,count FROM rating WHERE id IN %s', [tuple(rating_ids)])
        rows = cursor.fetchall()
        max_count_id = max(rows, key=operator.itemgetter(1))[0]

        return max_count_id


def awesome_function(duplicate_rows, column_index, taken_values_count):
    candidate_indexes = [x for x in range(0, len(duplicate_rows)) if duplicate_rows[x][column_index] is not None]
    row_index = get_most_used_row(candidate_indexes, taken_values_count)

    return row_index


def get_most_used_row(candidate_indexes, taken_values_count):
    if not candidate_indexes:
        candidate_indexes = taken_values_count.keys()
    return max({x: taken_values_count[x] for x in candidate_indexes}.iteritems(), key=operator.itemgetter(1))[0]


def string_length(string):
    if string:
        return len(string)

    return 0


if __name__ == "__main__":
    clean_movie()
