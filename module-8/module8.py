import mysql.connector

CONFIG = {
    "user": "root",
    "password": "mozgov-dejnix-7muzhI",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True,
    "autocommit": True
}


def run_query(table, query=None):
    if not query:
        print(f'-- DISPLAYING FIELDS from {table} --')
        query = f'SELECT * FROM {table}'
        if table == 'film':
            query = SELECTION

    with mysql.connector.connect(**CONFIG) as db:
        with db.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            for row in data:
                for key, value in row.items():
                    print(f'{key}: {value}')
                print('')


def insert_sql(table, headers, values):
    print(f'-- INSERTING VALUES into {table}--')
    insert = (
        f'INSERT INTO {table} '
        f'{headers} '
        f'VALUES {values}'
    )
    run_query(table, insert)
    run_query(table)


def delete_sql(table, criteria):
    print(f'-- DELETING VALUES in {table}--')
    delete = (
        f'DELETE FROM {table} '
        f'WHERE {criteria}'
    )
    run_query(table, delete)
    run_query(table)


def update_sql(table, update, criteria):
    print(f'-- UPDATING VALUES in {table}--')
    update = (
        f'UPDATE {table} '
        f'SET {update} '
        f'WHERE {criteria}'
    )
    run_query(table, update)
    run_query(table)


SELECTION = ('SELECT film_name as Name, film_director as Director, genre_name as Genre, '
             'studio_name as "Studio Name" FROM film '
             'INNER JOIN genre ON film.genre_id = genre.genre_id '
             'INNER JOIN studio ON film.studio_id = studio.studio_id')

run_query('film', SELECTION)
insert_sql('genre', '(genre_id, genre_name)', '("4", "Animation")')
insert_sql('studio', '(studio_id, studio_name)', '("4", "Pixar")')
insert_sql('film', '(film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)',
           '("Inside Out 2", "2024", "96", "Kelsey Mann", "4", "4")')
update_sql('film', 'genre_id = 1', 'film_name = "Alien"')
delete_sql('film', 'film_name = "Gladiator"')
