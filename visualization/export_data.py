import json
import psycopg2
import pycountry


def export():
    connection = psycopg2.connect(host='localhost',
                                  port=5432,
                                  user='soldag',
                                  database='integrated')
    cursor = connection.cursor()

    print 'Export budget statistics'
    js_output = export_budget_stats(cursor)

    print
    print 'Export revenue statistics'
    js_output += export_revenue_stats(cursor)

    print
    print 'Export participant statistics'
    js_output += export_participants_stats(cursor)

    print
    print 'Write statistics to "data.js"'
    js_file = open('data.js', 'w')
    js_file.write(js_output)
    js_file.close()


def export_budget_stats(cursor):
    cursor.execute('SELECT country.name, AVG(movie.budget) AS average '
                   'FROM movie, country, movie_country '
                   'WHERE movie.id = movie_country.movie_id AND movie_country.country_id = country.id AND movie.budget > 0 '
                   'GROUP BY country.name '
                   'ORDER BY average DESC')

    stats = {}
    for row in cursor.fetchall():
        country_code = get_country_code(row[0])
        if country_code:
            budget = row[1]
            stats[country_code] = float(budget)

    return 'var budgetData=%s;\n' % json.dumps(stats)


def export_revenue_stats(cursor):
    cursor.execute('SELECT country.name, AVG(movie.revenue) AS average '
                   'FROM movie, country, movie_country '
                   'WHERE movie.id = movie_country.movie_id AND movie_country.country_id = country.id AND movie.revenue > 0 '
                   'GROUP BY country.name '
                   'ORDER BY average DESC')

    stats = {}
    for row in cursor.fetchall():
        country_code = get_country_code(row[0])
        if country_code:
            revenue = row[1]
            stats[country_code] = float(revenue)

    return 'var revenueData=%s;\n' % json.dumps(stats)


def export_participants_stats(cursor):
    cursor.execute('SELECT country.name, AVG(sub.count) AS average '
                   'FROM movie_country,country,'
                   '('
                       'SELECT person_movie.movie_id AS movie_id, COUNT(*) AS count '
                       'FROM person_movie '
                       'GROUP BY person_movie.movie_id'
                   ') AS sub '
                   'WHERE movie_country.movie_id = sub.movie_id AND country.id = movie_country.country_id '
                   'GROUP BY country.name '
                   'ORDER BY average')

    stats = {}
    for row in cursor.fetchall():
        country_code = get_country_code(row[0])
        if country_code:
            participants_count = row[1]
            stats[country_code] = float(participants_count)

    return 'var participantData=%s;\n' % json.dumps(stats)


def get_country_code(country_name):
    try:
        return pycountry.countries.get(name=country_name).alpha2
    except KeyError:
        print 'Code of country "%s" could not be resolved!' % country_name

if __name__ == "__main__":
    export()
