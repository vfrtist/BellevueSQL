import mysql.connector

CONFIG = {
    "user": "root",
    "password": "mozgov-dejnix-7muzhI",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}


def run_query(table, query):
    print(f'\n-- DISPLAYING {table} RECORDS --')
    with mysql.connector.connect(**CONFIG) as db:
        with db.cursor() as cursor:
            cursor.execute(query)
            headers = [i[0] for i in cursor.description]
            for row in cursor:
                row = list(row)
                for count, column in enumerate(headers):
                    print(f'{column}: {row[count]}')
                print('')


run_query('studio', 'SELECT * FROM studio')
run_query('genre', 'SELECT * FROM genre')
run_query('film', 'SELECT * FROM film WHERE film_runtime < 120')
run_query('film', 'SELECT film_name, film_director FROM film ORDER BY film_director')
