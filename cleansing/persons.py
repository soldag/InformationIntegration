from __future__ import division
import re
import math
import operator
import psycopg2
from Levenshtein.StringMatcher import StringMatcher


SIMILARITY_THRESHOLD = 0.95


def clean_person():
    connection = psycopg2.connect(host='localhost',
                                  port=5432,
                                  user='Rosa',
                                  database='infint_integrated')

    # Get database cursors
    select_cursor = connection.cursor()
    edit_cursor = connection.cursor()

    print "Apply blocking 1"
    duplicates_count = split_into_blocks_stage_name(select_cursor, edit_cursor,
                                         'SELECT SUBSTR(LOWER(sub.stage_name),1,1) AS stage FROM (SELECT * FROM person WHERE first_name IS NULL AND last_name IS NULL AND gender IS NULL) AS sub GROUP BY stage',
                                        None)

    print "Apply blocking 2"
    duplicates_count += split_into_blocks_stage_name(select_cursor, edit_cursor,
                                          'SELECT SUBSTR(LOWER(sub.stage_name),1,1) AS stage FROM (SELECT * FROM person WHERE first_name IS NULL AND last_name IS NULL AND gender = \'m\') AS sub GROUP BY stage',
                                          'm')

    print "Apply blocking 3"
    duplicates_count += split_into_blocks_stage_name(select_cursor, edit_cursor,
                                          'SELECT SUBSTR(LOWER(sub.stage_name),1,1) AS stage FROM (SELECT * FROM person WHERE first_name IS NULL AND last_name IS NULL AND gender = \'f\') AS sub GROUP BY stage',
                                          'f')
    print "Apply blocking 4"
    duplicates_count += split_into_blocks(select_cursor, edit_cursor,
                                          'SELECT SUBSTR(LOWER(last_name),1,2) AS last, SUBSTR(LOWER(first_name),1,2) AS first, gender FROM person WHERE first_name IS NOT NULL OR last_name IS NOT NULL GROUP BY last, first, gender')

    # Commit all database changes
    # connection.commit()

    print('%d duplicates found.' % duplicates_count)


def split_into_blocks_stage_name(select_cursor, edit_cursor, query, gender):

    duplicates_count = 0
    select_cursor.execute(query)
    for group in select_cursor.fetchall():
        if gender is None:
            select_cursor.execute('SELECT * FROM person WHERE stage_name LIKE %s AND first_name IS NULL AND last_name IS NULL AND gender IS NULL', [group[0]+'%'])
        else:
            select_cursor.execute('SELECT * FROM person WHERE stage_name LIKE %s AND first_name IS NULL AND last_name IS NULL AND gender = %s', [group[0]+'%', gender])
        row_bucket = select_cursor.fetchall()
        duplicates_count += find_duplicates(edit_cursor, row_bucket)

    return duplicates_count


def split_into_blocks(select_cursor, edit_cursor, query):

    duplicates_count = 0
    select_cursor.execute(query)
    for group in select_cursor.fetchall():
        last_name = group[0]
        first_name = group[1]
        gender = group[2]
        if gender is None:
            if last_name is None:
                select_cursor.execute('SELECT * FROM person WHERE last_name IS NULL AND first_name LIKE %s AND gender IS NULL', [first_name+'%'])
            elif first_name is None:
                select_cursor.execute('SELECT * FROM person WHERE last_name LIKE %s AND first_name IS NULL AND gender IS NULL', [last_name+'%'])
            else:
                select_cursor.execute('SELECT * FROM person WHERE last_name LIKE %s AND first_name LIKE %s AND gender IS NULL', [last_name+'%', first_name+'%'])
        elif last_name is None:
            select_cursor.execute('SELECT * FROM person WHERE last_name IS NULL AND first_name LIKE %s AND gender LIKE %s', [first_name+'%', gender])
        elif first_name is None:
            select_cursor.execute('SELECT * FROM person WHERE last_name LIKE %s AND first_name IS NULL AND gender LIKE %s', [last_name+'%', gender])
        else:
            select_cursor.execute('SELECT * FROM person WHERE last_name LIKE %s AND first_name LIKE %s AND gender LIKE %s', [last_name+'%', first_name+'%', gender])
        row_bucket = select_cursor.fetchall()
        duplicates_count += find_duplicates(edit_cursor, row_bucket)

    return duplicates_count


def find_duplicates(cursor, rows):
    checked_rows = []
    duplicates_count = 0
    for i in range(0, len(rows)):
        row1 = rows[i]
        if row1:
            duplicate_rows = [row1]
            for j in range(i + 1, len(rows)):
                row2 = rows[j]
                if row2 and row2[0] not in checked_rows:
                    similarity = calculate_similarity(row1, row2)
                    if similarity >= SIMILARITY_THRESHOLD:
                        duplicate_rows.append(row2)
                    checked_rows.append(row2[0])
            if len(duplicate_rows) > 1:
                merge_duplicates(cursor, duplicate_rows)
                duplicates_count += len(duplicate_rows)

    return duplicates_count


def calculate_similarity(row1, row2):
    first_name_sim = name_similarity(row1[1], row2[1])
    last_name_sim = name_similarity(row1[2], row2[2])
    stage_name_sim = name_similarity(row1[3], row2[3])
    birth_year_sim = year_sim(row1[15], row2[15])
    death_year_sim = year_sim(row1[18], row2[18])
    imdb_id_sim = imdb_id_similarity(row1[20], row2[20])

    return 0.3 * imdb_id_sim + 0.25 * first_name_sim + 0.25 * last_name_sim + 0.1 * stage_name_sim + 0.05 * birth_year_sim + 0.05 * death_year_sim


def name_similarity(title1, title2):
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
    person_id = duplicate_rows[0][0]
    first_name, last_name, stage_name = get_names(duplicate_rows, 1, 2, 3, taken_values_count)
    gender = duplicate_rows[awesome_function(duplicate_rows, 4, taken_values_count)][4]
    work_count = get_largest_number(duplicate_rows, 5, taken_values_count)
    fan_count = get_largest_number(duplicate_rows, 6, taken_values_count)
    author_followers_count = get_largest_number(duplicate_rows, 7, taken_values_count)
    goodreads_author = get_goodreads_author(duplicate_rows, 8, taken_values_count)
    comment = concat_attributes(duplicate_rows, 9)
    rating_id = get_rating_id(duplicate_rows, 10, cursor)
    profile_photo_path = get_longest_string(duplicate_rows, 11, taken_values_count)
    biography = concat_attributes(duplicate_rows, 12)
    birth_day, birth_month, birth_year = get_date(duplicate_rows, 13, 14, 15, taken_values_count)
    death_day, death_month, death_year = get_date(duplicate_rows, 16, 17, 18, taken_values_count)
    homepage = duplicate_rows[awesome_function(duplicate_rows, 19, taken_values_count)][19]
    imdb_id = duplicate_rows[awesome_function(duplicate_rows, 20, taken_values_count)][20]
    place_of_birth = duplicate_rows[awesome_function(duplicate_rows, 21, taken_values_count)][21]
    place_of_death = duplicate_rows[awesome_function(duplicate_rows, 22, taken_values_count)][22]
    popularity = get_largest_number(duplicate_rows, 23, taken_values_count)
    start_work_year, end_work_year = get_work_years(duplicate_rows, 24, 25, taken_values_count)
    origin_country_id = duplicate_rows[awesome_function(duplicate_rows, 26, taken_values_count)][26]
    photo = duplicate_rows[awesome_function(duplicate_rows, 27, taken_values_count)][27]
    occupation = duplicate_rows[awesome_function(duplicate_rows, 28, taken_values_count)][28]

    # Update values of first duplicate row
    cursor.execute('UPDATE person SET first_name = %s, last_name = %s, stage_name = %s, gender = %s, work_count = %s,' 
                        'fan_count = %s, author_followers_count = %s, goodreads_author  = %s, comment  = %s, rating_id  = %s,'
                        ' profile_photo_path = %s, biography = %s, birth_day = %s, birth_month = %s, birth_year = %s, death_day = %s, death_month = %s, '
                        'death_year = %s, homepage = %s, imdb_id  = %s, place_of_birth = %s, place_of_death = %s, popularity = %s, start_work_year = %s, end_work_year = %s, '
                        'origin_country_id = %s, photo = %s, occupation = %s WHERE id=%s',
                   [
                       first_name, last_name, stage_name, gender, work_count, fan_count,
                       author_followers_count, goodreads_author, comment, rating_id, profile_photo_path, 
                       biography, birth_day, birth_month, birth_year, death_day, death_month, death_year, homepage, imdb_id, place_of_birth, place_of_death,
                       popularity, start_work_year, end_work_year, origin_country_id, photo, occupation, person_id
                   ])

    # Remove other duplicate rows
    dup_person_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != person_id])
    for dup_person_id in dup_person_ids:
        cursor.execute('SELECT movie_id, job_id, role FROM person_movie WHERE person_id=%s', [dup_person_id])
        for row in cursor.fetchall():
            movie_id = row[0]
            job_id = row[1]
            role = row[2]
            cursor.execute('SELECT * FROM person_movie WHERE person_id=%s AND movie_id=%s AND job_id=%s AND role=%s', [person_id, movie_id, job_id, role])
            if cursor.fetchone():
                cursor.execute('DELETE FROM person_movie WHERE person_id=%s AND movie_id=%s AND job_id=%s AND role=%s', [dup_person_id, movie_id, job_id, role])
            else:
                cursor.execute('UPDATE person_movie SET person_id=%s WHERE person_id=%s AND movie_id=%s AND job_id=%s AND role=%s',
                               [person_id, dup_person_id, movie_id, job_id, role])

    dup_person_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != person_id])
    for dup_person_id in dup_person_ids:
        cursor.execute('SELECT work_id FROM work_person WHERE person_id=%s', [dup_person_id])
        for row in cursor.fetchall():
            work_id = row[0]
            cursor.execute('SELECT * FROM work_person WHERE person_id=%s AND work_id=%s', [person_id, work_id])
            if cursor.fetchone():
                cursor.execute('DELETE FROM work_person WHERE person_id=%s AND work_id=%s', [dup_person_id, work_id])
            else:
                cursor.execute('UPDATE work_person SET person_id=%s WHERE person_id=%s AND work_id=%s',
                               [person_id, dup_person_id, work_id])

    dup_person_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != person_id])
    for dup_person_id in dup_person_ids:
        cursor.execute('SELECT artist_credit_id FROM artist_credit_name WHERE person_id=%s', [dup_person_id])
        for row in cursor.fetchall():
            artist_credit_id = row[0]
            cursor.execute('SELECT * FROM artist_credit_name WHERE person_id=%s AND artist_credit_id=%s AND job_id=%s AND role=%s', [person_id, artist_credit_id])
            if cursor.fetchone():
                cursor.execute('DELETE FROM artist_credit_name WHERE person_id=%s AND artist_credit_id=%s AND job_id=%s AND role=%s', [dup_person_id, artist_credit_id])
            else:
                cursor.execute('UPDATE artist_credit_name SET person_id=%s WHERE person_id=%s AND artist_credit_id=%s AND job_id=%s AND role=%s',
                               [person_id, dup_person_id, artist_credit_id])

    cursor.execute('DELETE FROM person WHERE id IN %s', [dup_person_ids])

    # rating
    rating_ids = tuple([person_id for person_id in [row[10] for row in duplicate_rows] if person_id is not None and person_id != rating_id])
    if rating_ids:
        cursor.execute('DELETE FROM rating WHERE id IN %s', [rating_ids])


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


def get_goodreads_author(duplicate_rows, goodreads_author_column_index, taken_values_count):
    authors = [row[goodreads_author_column_index] for row in duplicate_rows]
    if 't' in authors:
        index = authors.indexOf('t')
    elif 'f' in authors:
        index = authors.indexOf('f')
    else:
        index = 0

    taken_values_count[index] += 1
    return authors[index]


def concat_attributes(duplicate_rows, column_index):
    values = [row[column_index] for row in duplicate_rows if row[column_index] is not None]
    return '; '.join(values)


def get_names(duplicate_rows, first_name_column_index, last_name_column_index, stage_name_column_index, taken_values_count):
    first_names = [x[first_name_column_index] for x in duplicate_rows]
    last_names = [x[last_name_column_index] for x in duplicate_rows]
    stage_names = [x[stage_name_column_index] for x in duplicate_rows]

    complete_names = [x for x in range(0, len(duplicate_rows)) if first_names[x] is not None and last_names[x] is not None and stage_names[x] is not None]
    half_complete_names = [x for x in range(0, len(duplicate_rows)) if first_names[x] is not None and last_names[x] is not None and stage_names[x] is None]
    second_half_complete_names = [x for x in range(0, len(duplicate_rows)) if first_names[x] is None and last_names[x] is not None and stage_names[x] is not None]
    third_half_complete_names = [x for x in range(0, len(duplicate_rows)) if first_names[x] is not None and last_names[x] is None and stage_names[x] is not None]
    if complete_names:
        first_name_index = last_name_index = stage_name_index = get_most_used_row(complete_names, taken_values_count)
    elif half_complete_names: 
        first_name_index = last_name_index = get_most_used_row(half_complete_names, taken_values_count)
        stage_name_index = awesome_function(duplicate_rows, stage_name_column_index, taken_values_count)
    elif second_half_complete_names:
        last_name_index = stage_name_index = get_most_used_row(second_half_complete_names, taken_values_count)
        first_name_index = awesome_function(duplicate_rows, first_name_column_index, taken_values_count)
    elif third_half_complete_names:
        first_name_index = stage_name_index = get_most_used_row(half_complete_names, taken_values_count)
        last_name_index = awesome_function(duplicate_rows, last_name_column_index, taken_values_count)
    else:
        first_name_index = awesome_function(duplicate_rows, first_name_column_index, taken_values_count)
        last_name_index = awesome_function(duplicate_rows, last_name_column_index, taken_values_count)
        stage_name_index = awesome_function(duplicate_rows, stage_name_column_index, taken_values_count)

    taken_values_count[first_name_index] += 1
    taken_values_count[last_name_index] += 1
    taken_values_count[stage_name_index] += 1
    return duplicate_rows[first_name_index][first_name_column_index], duplicate_rows[last_name_index][last_name_column_index], duplicate_rows[stage_name_index][stage_name_column_index]


def get_date(duplicate_rows, day_column_index, month_column_index, year_column_index, taken_values_count):
    days = [x[day_column_index] for x in duplicate_rows]
    months = [x[month_column_index] for x in duplicate_rows]
    years = [x[year_column_index] for x in duplicate_rows]

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
    return duplicate_rows[index][day_column_index], duplicate_rows[index][month_column_index], duplicate_rows[index][year_column_index]


def get_work_years(duplicate_rows, start_year_column_index, end_year_column_index, taken_values_count):
    start_years = [x[start_year_column_index] for x in duplicate_rows]
    end_years = [x[end_year_column_index] for x in duplicate_rows]

    complete_dates = [x for x in range(0, len(duplicate_rows)) if start_years[x] is not None and end_years[x] is not None]
    if complete_dates:
        start_year_index = end_year_index = get_most_used_row(complete_dates, taken_values_count)
    else:
        start_year_index = awesome_function(duplicate_rows, start_year_column_index, taken_values_count)
        end_year_index = awesome_function(duplicate_rows, end_year_column_index, taken_values_count)

    taken_values_count[start_year_index] += 1
    taken_values_count[end_year_index] += 1
    return duplicate_rows[start_year_index][start_year_column_index], duplicate_rows[end_year_index][end_year_column_index]


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
    clean_person()
