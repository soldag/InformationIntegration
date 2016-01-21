from __future__ import division
import re
import math
import operator
import psycopg2
from Levenshtein.StringMatcher import StringMatcher
from psycopg2._psycopg import cursor
from unidecode import unidecode

SIMILARITY_THRESHOLD = 0.95


def clean_works():
    connection = psycopg2.connect(host='localhost',
                                  port=5432,
                                  user='soldag',
                                  password='',
                                  database='integrated')

    # Get database cursors
    select_cursor = connection.cursor()
    edit_cursor = connection.cursor()

    print "Apply blocking 1"
    duplicates_count = split_into_blocks(select_cursor, edit_cursor,
                                         'SELECT COUNT(*) FROM work WHERE LOWER(title) LIKE %s',
                                         'SELECT * FROM work WHERE LOWER(title) LIKE %s ORDER BY title',
                                         ['the %'], 4)

    print "Apply blocking 2"
    duplicates_count += split_into_blocks(select_cursor, edit_cursor,
                                          'SELECT COUNT(*) FROM work WHERE LOWER(title) LIKE %s',
                                          'SELECT * FROM work WHERE LOWER(title) LIKE %s ORDER BY title',
                                          ['sonate%'], 6)

    print "Apply blocking 3"
    duplicates_count = split_into_blocks(select_cursor, edit_cursor,
                                          'SELECT COUNT(*) FROM work WHERE LOWER(title) NOT LIKE %s AND LOWER(title) NOT LIKE %s',
                                          'SELECT * FROM work WHERE LOWER(title) NOT LIKE %s AND LOWER(title) NOT LIKE %s ORDER BY title',
                                          ['the %', 'sonate%'])

    # Commit all database changes
    connection.commit()

    print('%d duplicates found.' % duplicates_count)


def split_into_blocks(select_cursor, edit_cursor, count_query, rows_query, arguments=None, title_offset=0):
    if arguments is None:
        arguments = []

    # Count rows in work table for calculating the progress
    i = 0
    last_process = -1
    select_cursor.execute(count_query, arguments)
    work_count = select_cursor.fetchone()[0]

    row_bucket = []
    current_first_char = None
    duplicates_count = 0

    select_cursor.execute(rows_query, arguments)
    while i < work_count:
        row = select_cursor.fetchone()

        # Check, if last row has been read
        if row is None:
            break

        title = unidecode(row[1][title_offset:])
        if title:
            first_title_char = title[:2].lower()
            if current_first_char != first_title_char:
                # First title character changed, so find duplicates in buckets
                if row_bucket and len(row_bucket) > 1:
                    print "Find duplicates...(%d)" % len(row_bucket)
                    duplicates_count += find_duplicates(edit_cursor, row_bucket)
                    row_bucket = []

                current_first_char = first_title_char
                print "Next characters: " + current_first_char

            row_bucket.append(row)

        # Calculate and print progress
        i += 1
        process = i / work_count * 100
        if last_process == -1 or process - last_process >= 1:
            print "%d%% completed" % int(math.floor(process))
            last_process = process

    # Find duplicates within last group
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
    title_sim = title_similarity(row1[1], row2[1])
    edition_information_sim = edition_information_similarity(row1[3], row2[3])
    isbn_sim = isbn_similarity(row1[5], row2[5], row1[6], row2[6])

    return 0.5 * isbn_sim + 0.3 * title_sim + 0.2 * edition_information_sim


def title_similarity(title1, title2):
    if any([x is None for x in [title1, title2]]):
        return 1

    return normalized_levenshtein(title1, title2)


def edition_information_similarity(value1, value2):
    if any([x is None for x in [value1, value2]]):
        return 1

    edition_number1 = get_edition_number(value1)
    edition_number2 = get_edition_number(value2)
    if edition_number1 and edition_number2:
        if edition_number1 == edition_number2:
            return 1
        else:
            return 0
    else:
        return normalized_levenshtein(value1, value2)


def isbn_similarity(isbn_1, isbn_2, isbn13_1, isbn13_2):
    if any([x is None for x in [isbn_1, isbn_2]]) and any([x is None for x in [isbn13_1, isbn13_2]]):
        return 1

    isbn_sim = levenshtein(isbn_1, isbn_2)
    isbn13_sim = levenshtein(isbn13_1, isbn13_2)

    if isbn_sim + isbn13_sim <= 1:
        if isbn_sim > 0:
            return 1 - isbn_sim / max_length(isbn_1, isbn_2)
        else:
            return 1 - isbn13_sim / max_length(isbn13_1, isbn13_2)

    return 0


def get_edition_number(edition_information):
    if edition_information is not None:
        pattern = re.compile('(?P<number>\d+)(st|nd|rd|th)( edition)?')
        match = pattern.match(edition_information)
        if match is not None:
            if 'number' in match.groupdict():
                return int(match.group('number'))

        text_ranks = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth', 'tenth']
        for rank in text_ranks:
            if rank in edition_information.lower():
                return text_ranks.index(rank) + 1


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
    work_id = duplicate_rows[0][0]
    title = get_longest_string(duplicate_rows, 1, taken_values_count)
    description = get_longest_string(duplicate_rows, 2, taken_values_count)
    isbn, isbn_13, isbn_index = get_isbn(duplicate_rows, 5, 6, taken_values_count)
    edition_information = get_edition_information(duplicate_rows, 3, 5, isbn_index, taken_values_count)
    format = duplicate_rows[awesome_function(duplicate_rows, 4, taken_values_count)][4]
    publisher = duplicate_rows[awesome_function(duplicate_rows, 11, taken_values_count)][11]
    day, month, year = get_date(duplicate_rows, taken_values_count)
    text_reviews_count = get_largest_number(duplicate_rows, 12, taken_values_count)
    rating_id = get_rating_id(duplicate_rows, 13, cursor)

    # Update values of first duplicate row
    cursor.execute('UPDATE work SET title=%s,description=%s,isbn=%s,isbn13=%s,edition_information=%s,'
                   'format=%s,publisher=%s,publication_day=%s,publication_month=%s,publication_year=%s,'
                   'text_reviews_count=%s,rating_id=%s WHERE id=%s',
                   [
                       title, description, isbn, isbn_13, edition_information, format,
                       publisher, day, month, year, text_reviews_count, rating_id, work_id
                   ])

    # Remove other duplicate rows
    dup_work_ids = tuple([id for id in [row[0] for row in duplicate_rows] if id != work_id])
    for dup_work_id in dup_work_ids:
        cursor.execute('SELECT person_id FROM work_person WHERE work_id=%s', [dup_work_id])
        for row in cursor.fetchall():
            person_id = row[0]
            cursor.execute('SELECT * FROM work_person WHERE work_id=%s AND person_id=%s', [work_id, person_id])
            if cursor.fetchone():
                cursor.execute('DELETE FROM work_person WHERE work_id=%s AND person_id=%s', [dup_work_id, person_id])
            else:
                cursor.execute('UPDATE work_person SET work_id=%s WHERE work_id=%s AND person_id=%s',
                               [work_id, dup_work_id, person_id])

    cursor.execute('DELETE FROM work WHERE id IN %s', [dup_work_ids])

    rating_ids = tuple([work_id for work_id in [row[13] for row in duplicate_rows] if work_id is not None and work_id != rating_id])
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
    return strings[index]


def get_largest_number(duplicate_rows, column_index, taken_values_count):
    numbers = [x[column_index] for x in duplicate_rows]
    index = numbers.index(max(numbers))

    taken_values_count[index] += 1
    return numbers[index]


def get_isbn(duplicate_rows, isbn_column_index, isbn_13_column_index, taken_values_count):
    list_isbn = [x[isbn_column_index] for x in duplicate_rows]
    list_isbn_13 = [x[isbn_13_column_index] for x in duplicate_rows]

    complete_isbn = [x for x in range(0, len(duplicate_rows)) if list_isbn[x] is not None and list_isbn_13[x] is not None]
    if complete_isbn:
        index = complete_isbn[0]
    else:
        sparse_isbn = [x for x in range(0, len(duplicate_rows)) if list_isbn[x] is not None or list_isbn_13[x] is not None]
        if sparse_isbn:
            index = sparse_isbn[0]
        else:
            index = 0

    taken_values_count[index] += 1
    return list_isbn[index], list_isbn_13[index], index


def get_edition_information(duplicate_rows, column_index, isbn_column_index, merged_isbn_index, taken_values_count):
    edition_information = [x[column_index] for x in duplicate_rows]

    if duplicate_rows[merged_isbn_index][isbn_column_index]:
        index = merged_isbn_index
    else:
        index = awesome_function(duplicate_rows, column_index, taken_values_count)

    taken_values_count[index] += 1
    return edition_information[index]


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
    clean_works()
