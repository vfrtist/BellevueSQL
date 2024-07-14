import mysql.connector

PASSWORD = 'mozgov-dejnix-7muzhI'

CONFIG = {
    "user": "root",
    "password": PASSWORD,
    "host": "127.0.0.1",
    "raise_on_warnings": True,
    "autocommit": True
}

# Reset database for testing purposes
with mysql.connector.connect(**CONFIG) as db:
    with db.cursor(dictionary=True) as cursor:
        cursor.execute('DROP DATABASE bacchus')


class Table:
    '''
    Creates a table with a primary key <table_name>_id.
    '''

    def __init__(self, database, table):
        self._database = database
        self._config = CONFIG
        self._table = table
        self.route_to_table()

    def __repr__(self):
        return f'{self._table}'

    def route_to_table(self):
        '''
        Connects to the database and creates the table.
        '''
        print(f'\nInitializing {self._table}')
        self.query(f'CREATE DATABASE IF NOT EXISTS {self._database}')
        self._config['database'] = self._database
        self.query(
            f'CREATE TABLE IF NOT EXISTS {self._table} ('
            f'{self._table}_id INT NOT NULL AUTO_INCREMENT,'
            f'PRIMARY KEY({self._table}_id))'
        )
        print(f'Connected to - {self._table}')

    def connect(self):
        '''
        Returns a connection to the database.
        '''
        try:
            return mysql.connector.connect(**self._config)
        except mysql.connector.Error as err:
            print(err)

    @staticmethod
    def print_results(results):
        '''
        Prints query results.
        '''
        for row in results:
            for key, value in row.items():
                print(f'{key}: {value}')
            print('\n')

    def query(self, query=None):
        '''
        Runs the query and prints results.
        '''
        if not query:
            query = f'SELECT * FROM {self._table}'
        with self.connect() as db:
            with db.cursor(dictionary=True) as cursor:
                try:
                    cursor.execute(query)
                    self.print_results(cursor.fetchall())
                except:
                    print(f'{query} was not executed')

    def insert(self, headers, *values):
        '''
        Inserts values into the table.
        '''
        print(f'\n-- INSERTING VALUES into {self._table} --')
        for val in values:
            insert = (
                f'INSERT INTO {self._table} '
                f'({headers}) '
                f'VALUES ({val})'
            )
            self.query(insert)

    def delete(self, criteria=None):
        '''
        Deletes rows matching the criteria.
        '''

        print(f'\n-- DELETING VALUES in {self._table} --')
        delete = (
            f'DELETE FROM {self._table} '
        )
        if criteria:
            delete += f'WHERE {criteria}'
        self.query(delete)

    def update(self, update, criteria):
        '''
        Updates rows matching the criteria.
        '''
        print(f'\n-- UPDATING VALUES in {self._table} --')
        update = (
            f'UPDATE {self._table} '
            f'SET {update} '
            f'WHERE {criteria}'
        )
        self.query(update)

    def select(self, values=None, criteria=None):
        '''
        Selects and prints rows matching the criteria.
        '''
        if not values:
            print(f'\n-- SELECTING all from {self._table} --')
            self.query()
        else:
            print(f'\n-- SELECTING {values} from {self._table} --')
            selection = f'SELECT {values} FROM {self._table}'
            if criteria:
                selection += f' WHERE {criteria}'
            self.query(selection)

    def show(self):
        '''
        Shortcut to show the table content.
        '''
        self.select()

    def add_columns(self, *columns):
        '''
        Adds columns to the table.
        '''
        for col in columns:
            self.query(f'ALTER TABLE {self._table} ADD {col}')

    def remove_columns(self, *columns):
        '''
        Removes columns from the table.
        '''
        for col in columns:
            self.query(f'ALTER TABLE {self._table} DROP COLUMN {col}')

    def link_foreign_key(self, col_name, linked_table, linked_col_name):
        '''
        Adds a foreign key to the table.
        '''
        self.query(f'ALTER TABLE {self._table} ADD FOREIGN KEY({col_name}) '
                   f'REFERENCES {linked_table}({linked_col_name})'
                   )


# Design
employees = Table('bacchus', 'employees')
employees.add_columns(
    'employees_name VARCHAR(75) NOT NULL', 'employees_role VARCHAR(75)'
)
employees.insert(
    'employees_id, employees_name, employees_role',
    '"1", "Janet Collins", "Finances"', '"2", "Roz Murphy", "Marketing"',
    '"3", "Bob Ulrich", "Marketing Assistant"', '"4", "Henry Doyle", "Production Manager"',
    '"5", "Maria Costanza", "Distribution"', '"6", "Stan Bacchus", "Owner"', '"7", "Davis Bacchus", "Owner"'
)

task_logs = Table('bacchus', 'task_logs')
task_logs.add_columns(
    'task_logs_date DATE',
    'task_logs_details VARCHAR(250) NOT NULL', 'employees_id INT NOT NULL'
)
task_logs.insert(
    'task_logs_date, task_logs_details, employees_id',
    '"2024-02-05", "Prepare quarterly report", "1"',
    '"2024-02-12", "Organize marketing event", "2"'
)

supplies = Table('bacchus', 'supplies')
supplies.add_columns(
    'supplies_description VARCHAR(75)', 'suppliers_id INT NOT NULL'
)
supplies.insert(
    'supplies_description, suppliers_id',
    '"Bottles", "1"', '"Corks", "1"',
    '"Labels", "2"', '"Boxes", "2"',
    '"Vats", "3"', '"Tubing", "3"'
)

inventory = Table('bacchus', 'inventory')
inventory.add_columns(
    'inventory_qty INT DEFAULT 0', 'supplies_id INT NOT NULL',
    'product_id INT NOT NULL', 'inventory_verification DATE NOT NULL'
)
inventory.insert(
    'inventory_qty, supplies_id, product_id, inventory_verification',
    '"600", "1", "1", "2024-02-01"',
    '"400", "3", "2", "2024-02-02"'
)

suppliers = Table('bacchus', 'suppliers')
suppliers.add_columns(
    'suppliers_name VARCHAR(75) NOT NULL', 'suppliers_contact_info VARCHAR(250) NOT NULL'
)
suppliers.insert(
    'suppliers_id, suppliers_name, suppliers_contact_info',
    '"1", "Bottle It Up", "Nick Cork"', '"2", "Pack It", "Sarah Box"',
    '"3", "Global Tubing", "Carl Vats"'
)

products = Table('bacchus', 'products')
products.add_columns(
    'products_name VARCHAR(75) NOT NULL', 'products_description VARCHAR(250) NOT NULL'
)
products.insert(
    'products_name, products_description',
    '"Merlot", "Red Wine"',
    '"Cabernet", "Red Wine"',
    '"Chablis", "White Wine"',
    '"Chardonnay", "White Wine"'
)

product_logs = Table('bacchus', 'product_logs')
product_logs.add_columns(
    'products_id INT NOT NULL', 'product_logs_start_time DATETIME NOT NULL',
    'product_logs_end_time DATETIME NOT NULL', 'products_issues VARCHAR(250) NOT NULL'
)
product_logs.insert(
    'products_id, product_logs_start_time, product_logs_end_time, products_issues',
    '"1", "2024-02-15 09:00:00", "2024-02-15 17:00:00", "None"',
    '"2", "2024-02-20 09:00:00", "2024-02-20 17:00:00", "Temperature fluctuation"'
)

distributors = Table('bacchus', 'distributors')
distributors.add_columns(
    'distributors_name VARCHAR(75) NOT NULL', 'distributors_contact_info VARCHAR(250) NOT NULL',
    'distributors_history VARCHAR(250) NOT NULL', 'distributors_preferences VARCHAR(250) NOT NULL'
)
distributors.insert(
    'distributors_name, distributors_contact_info, distributors_history, distributors_preferences',
    '"Vineyard Ventures", "Alex Green, alex.green@example.com", "Long history of timely deliveries", "Prefers red wines"',
    '"Wine Distributors Inc.", "Casey Blue, casey.blue@example.com", "Excellent reputation", "Prefers white wines"'
)

orders = Table('bacchus', 'orders')
orders.add_columns(
    'distributors_id INT NOT NULL', 'orders_date DATE NOT NULL', 'orders_status VARCHAR(250) NOT NULL'
)
orders.insert(
    'distributors_id, orders_date, orders_status',
    '"1", "2024-03-10", "Shipped"',
    '"2", "2024-03-20", "Pending"'
)

order_details = Table('bacchus', 'order_details')
order_details.add_columns(
    'orders_id INT NOT NULL', 'products_id INT NOT NULL', 'order_details_qty INT DEFAULT 1'
)
order_details.insert(
    'orders_id, products_id, order_details_qty',
    '"1", "1", "200"',
    '"2", "3", "150"'
)

# linking foreign keys
task_logs.link_foreign_key('employees_id', 'employees', 'employees_id')
supplies.link_foreign_key('suppliers_id', 'suppliers', 'suppliers_id')
product_logs.link_foreign_key('products_id', 'products', 'products_id')
order_details.link_foreign_key('orders_id', 'orders', 'orders_id')
order_details.link_foreign_key('products_id', 'products', 'products_id')
orders.link_foreign_key('distributors_id', 'distributors', 'distributors_id')

# show all tables content
TABLES = [employees, task_logs, supplies, inventory, suppliers,
          products, product_logs, distributors, orders, order_details]

for table in TABLES:
    table.show()
