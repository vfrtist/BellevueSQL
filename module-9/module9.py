import mysql.connector

PASSWORD = 'mozgov-dejnix-7muzhI'

CONFIG = {
    "user": "root",
    "password": PASSWORD,
    "host": "127.0.0.1",
    "raise_on_warnings": True,
    "autocommit": True
}


class Table:
    '''
    New tables or databases will automatically generate with <table name>_id as an auto increment integer as the only column
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
        Initial connection function
        '''
        print(f'\nInitializing {self._table}')
        self.query(f'CREATE DATABASE {self._database}')
        self._config['database'] = self._database
        self.query(
            f'CREATE TABLE {self._table} ('
            f'{self._table}_id INT NOT NULL AUTO_INCREMENT,'
            f'PRIMARY KEY({self._table}_id))'
        )
        print(f'Connected to - {self._table}')

    def connect(self):
        '''
        Returns a connection to the table.
        '''
        try:
            return mysql.connector.connect(**self._config)
        except mysql.connector.Error as err:
            print(err)

    @staticmethod
    def print_results(results):
        '''
        A loop to print results from queries
        '''
        for row in results:
            for key, value in row.items():
                print(f'{key}: {value}')
            print('\n')

    def query(self, query=None):
        '''
        Runs the query
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
        All values as strings\n
        Headers in a list: self_id, self_value\n
        Values in in quotes in a list: "4", "Test"\n
        Can enter multiple people values with a single list of headers
        '''
        print(f'\n-- INSERTING VALUES into {self._table} --')
        for val in values:
            insert = (
                f'INSERT INTO {self._table} '
                f'({headers}) '
                f'VALUES ({val})'
            )
            self.query(insert)

    def delete(self, criteria):
        '''
        Enter as a string\n
        criteria fills in after WHERE as string
        '''
        print(f'\n-- DELETING VALUES in {self._table} --')
        delete = (
            f'DELETE FROM {self._table} '
            f'WHERE {criteria}'
        )
        self.query(delete)

    def update(self, update, criteria):
        '''
        Enter as a string\n
        Update is the change to make A = B\n
        Criteria is the where
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
        Enter as a string\n
        Column As Name, Inner Join etc.\n
        Leave blank to select all \n
        WHERE is optional
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
        Shortcut to show table.
        '''
        self.select()

    def add_columns(self, *columns):
        '''
        Values as <name> DATATYPE DEFAULT <val>
        '''
        for col in columns:
            self.query(f'ALTER TABLE {self._table} ADD {col}')

    def remove_columns(self, *columns):
        '''
        Column names that match will be removed.
        '''
        for col in columns:
            self.query(f'ALTER TABLE {self._table} DROP COLUMN {col}')

    def link_foreign_key(self, col_name, linked_table, linked_col_name):
        '''
        Enter all values as string \n
        Foreign key name autocreated as fk_{linked_table}
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
    'employees_name, employees_role',
    '"Janet Collins", "Finances"', '"Roz Murphy", "Marketing"',
    '"Bob Ulrich", "Marketing Assistant"', '"Henry Doyle", "Production Manager"',
    '"Maria Costanza", "Distribution"', '"Stan Bacchus", "Owner"', '"Davis Bacchus", "Owner"'
)

task_logs = Table('bacchus', 'task_logs')
task_logs.add_columns(
    'task_logs_date DATE',
    'task_logs_details VARCHAR(250) NOT NULL', 'employees_id INT NOT NULL'
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

suppliers = Table('bacchus', 'suppliers')
suppliers.add_columns(
    'suppliers_name VARCHAR(75) NOT NULL', 'suppliers_contact_info VARCHAR(250) NOT NULL'
)
suppliers.remove_columns('suppliers_deliveries')
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
    'product_logs_end_time DATETIME NOT NULL', 'products_issues VARCHAR(250) NOT NULL DEFAULT NONE'
)


distributors = Table('bacchus', 'distributors')
distributors.add_columns(
    'distributors_name VARCHAR(75) NOT NULL', 'distributors_contact_info VARCHAR(250) NOT NULL',
    'distributors_history VARCHAR(250) NOT NULL', 'distributors_preferences VARCHAR(250) NOT NULL'
)

orders = Table('bacchus', 'orders')
orders.add_columns(
    'distributors_id INT NOT NULL', 'orders_date DATE NOT NULL', 'orders_status VARCHAR(250) NOT NULL'
)

order_details = Table('bacchus', 'order_details')
order_details.add_columns(
    'orders_id INT NOT NULL', 'products_id INT NOT NULL', 'order_details_qty INT DEFAULT 1'
)

# linking
task_logs.link_foreign_key('employees_id', 'employees', 'employees_id')
supplies.link_foreign_key('suppliers_id', 'suppliers', 'suppliers_id')
product_logs.link_foreign_key('products_id', 'products', 'products_id')
order_details.link_foreign_key('orders_id', 'orders', 'orders_id')
order_details.link_foreign_key('products_id', 'products', 'products_id')
orders.link_foreign_key('distributors_id', 'distributors', 'distributors_id')

# Collection for ease later
TABLES = [employees, task_logs, supplies, inventory, suppliers,
          products, distributors, orders, order_details]

for table in TABLES:
    table.show()
