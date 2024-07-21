import mysql.connector
from mysql.connector import FieldType

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
    Python SQL Table class for functionality
    '''

    def __init__(self, database, table):
        self._database = database
        self._config = CONFIG
        self._table = table
        self.columns = {}
        self.route_to_table()

    def __repr__(self):
        return f'{self._table}'

    def route_to_table(self):
        '''
        Connects to the database and creates the table.
        '''
        print(f'\nInitializing {self._table}')
        self.query(f'CREATE DATABASE IF NOT EXISTS {self._database}', True)
        self._config['database'] = self._database
        self.query(
            f'CREATE TABLE IF NOT EXISTS {self._table} ('
            f'{self._table}_id INT NOT NULL AUTO_INCREMENT,'
            f'PRIMARY KEY({self._table}_id))', True
        )
        print(f'Connected to - {self._table}\n')
        self.get_columns()

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

    @staticmethod
    def fix_values(value):
        '''
        method to true value
        '''
        fixed_values = ''
        for val in value:
            if not val:
                fixed_values += 'NULL, '
            else:
                fixed_values += f'"{str(val)}", '
        fixed_values = fixed_values[:-2]
        return fixed_values

    def fix_columns(self, value: tuple):
        if len(value) == 3:
            return (value[0], value[1])
        return (f'{self._table}_{value[0]}', value[1])

    def query(self, query: str = None, silent=False):
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
                    return True
                except:
                    if not silent:
                        print(f'{query} was not executed')
                    return False

    def insert(self, values: list, include_id=False):
        '''
        Inserts values into the table.\n
        Values are a list of tuples allowing multiple line entries at once\n
        Headers are populated automatically from the table\n
        The default is to *not* include_id, because this auto-updates and often doesn't matter\n
        Set include_id to True to manually input values for id, these cannot be duplicates or they will fail\n
        '''
        columns = list(self.columns.keys())
        if not include_id:
            columns = columns[1:]
        fixed_columns = ', '.join(columns)
        for val in values:
            # SQlify values
            fixed_values = self.fix_values(val)
            # Check for missing data
            if len(columns) != len(val):
                print('A value is missing see below')
                print(f'-- Columns: {fixed_columns}')
                print(f'-- Values: {fixed_values}')
            else:
                insert = (
                    f'INSERT INTO {self._table} '
                    f'({fixed_columns}) '
                    f'VALUES ({fixed_values})'
                )
                if self.query(insert):
                    print(
                        f'-- {fixed_values} inserted into {self._table} successfully.')

        # for val in values:
        #     insert = (
        #         f'INSERT INTO {self._table} '
        #         f'({headers}) '
        #         f'VALUES ({val})'
        #     )
        #     if self.query(insert):
        #         print(f'-- {val} inserted into {self._table} successfully.')

    def delete(self, criteria: str = None):
        '''
        Deletes rows matching the criteria.\n
        Leave empty to clear all data
        '''
        delete = (
            f'DELETE FROM {self._table} '
        )
        if criteria:
            delete += f'WHERE {criteria}'
        if self.query(delete):
            print(f'-- Rows in {self._table} deleted successfully')
            if not criteria:
                self.query(f'ALTER TABLE {self._table} AUTO_INCREMENT = 1')

    def update(self, update: str, criteria: str):
        '''
        Updates rows matching the criteria.
        '''
        update = (
            f'UPDATE {self._table} '
            f'SET {update} '
            f'WHERE {criteria}'
        )
        if self.query(update):
            print(f'-- {self._table} updated successfully')

    def select(self, values: str = None, joins: tuple = None, where: str = None, group_by: str = None, order_by: str = None, limit: int = None):
        '''
        Selects and prints rows\n
        Default is that all special modifiers are set to none.\n
        Any can be set independantly of every other\n
        Joins are a tuple of strings, or Tables which will be processed with the Table.join function\n
        '''
        # Values
        if not values:
            values = '*'
        selection = f'SELECT {values} FROM {self._table} '

        # Join
        if joins:
            for val in joins:
                if type(val) == Table:
                    selection += self.join(val)
                else:
                    selection += val

        # Where
        if where:
            selection += f'WHERE {where} '

        # Group
        if group_by:
            selection += f'GROUP BY {group_by} '

        # Order
        if order_by:
            selection += f'ORDER BY {order_by} '

        if limit:
            selection += f'LIMIT {limit} '

        self.query(selection)

    def show(self):
        '''
        Shortcut to show the table content.
        '''
        self.select()

    def add_columns(self, *columns: tuple):
        '''
        Adds columns to the table as tuples\n
        (Column Name, Column Type)\n\n
        ** DO NOT include table name in Column Name.**\n
        ** Table name is added automatically for consistency **\n
        ** You can add foreign keys directly with link_to() **\n\n
        Example: (Name, VARCHAR(250) NOT NULL)\n
        '''
        for col in columns:
            column_name, column_type = self.fix_columns(col)
            if column_name not in self.columns.keys():
                if self.query(f'ALTER TABLE {self._table} ADD {column_name.lower()} {column_type.upper()}'):
                    print(f'Added {column_name} to {self._table} successfully')
                    self.get_columns()
            else:
                print(f'{column_name} already exists in {self._table}\n')

    def rename(self, change_to: str, change_from: str = None):
        '''
        leave "change_from" blank to change table name\n
        Otherwise, put old column names in change_from, and new ones in change_to.
        '''
        query = f'ALTER TABLE {self._table} '
        if change_from:  # updating column
            if change_from in self.columns.keys():
                query += f'RENAME COLUMN {change_from} TO {change_to}'
            else:
                print(f'{change_from} is not in {self._table}\n')
                print('Please check the spelling')
                self.show_columns()
        else:  # Updating table
            query += f'RENAME TO {change_to}'
            self.rename(f'{change_to}_id', f'{self._table}_id')
            self._table = change_to

        if self.query(query):
            print(f'Renamed to {change_to} successfully')
            if change_from:
                self.get_columns()

    def drop_columns(self, *columns: str):
        '''
        Drops columns from the table.
        '''
        for col in columns:
            if self.query(f'ALTER TABLE {self._table} DROP COLUMN {col}'):
                print(f'{col} dropped from {self._table} successfully')
                del self.columns[col]

    def link_to(self, *tables):
        '''
        Adds and links a foreign key to the provided Tables
        '''
        for table in tables:
            link_id = f'{table}_id'
            self.add_columns((link_id, 'INT NOT NULL', True))
            if self.query(f'ALTER TABLE {self._table} ADD FOREIGN KEY({link_id}) '
                          f'REFERENCES {table}({link_id})'):
                print(f'{self._table} linked to {table} successfully')

    def get_columns(self):
        with self.connect() as db:
            with db.cursor(dictionary=True) as cursor:
                try:
                    cursor.execute(f'SELECT * FROM {self._table}')
                    cursor.fetchall()
                    self.columns = {
                        col[0]: FieldType.get_info(col[1]) for col in cursor.description}
                except:
                    print(f'Could not retrieve headers from {self._table}')

    def show_columns(self):
        print(f'-- {self._table} table columns --')
        for k, v in self.columns.items():
            print(f'\n  Column: {k}'
                  f'\n  Type: {v}')

    def join(self, *tables):
        '''
        Supply tables as tables\n
        An inner join is created where ID's match across tables assuming they reference the same thing.
        '''
        joint = ''
        me = set(self.columns.keys())
        for table in tables:
            them = set(table.columns.keys())
            shared_key = list(me.intersection(them))[0]
            if shared_key:
                joint += (f'INNER JOIN {table} ON '
                          f'{self._table}.{shared_key} = {table}.{shared_key} ')
        return joint


orders = Table('bacchus', 'orders')
order_details = Table('bacchus', 'order_details')
distributors = Table('bacchus', 'distributors')
products = Table('bacchus', 'products')
employees = Table('bacchus', 'employees')
hours = Table('bacchus', 'hours')
suppliers = Table('bacchus', 'suppliers')
supplies = Table('bacchus', 'supplies')
supply_orders = Table('bacchus', 'supply_orders')

print('---------- Wine Sales ----------')
order_details.select(
    values='products_name as Name, SUM(order_details_qty) as "Total Sold"', group_by='products_name', joins=(products,))

print('---------- Which distributor carries which wine? ----------')
orders.select(values='distributors_name as Distributor, products_name as Product, SUM(order_details_qty) as "Total Ordered"',
              joins=(order_details, distributors,
                     order_details.join(products)),
              group_by='distributors_name, products_name',
              order_by='distributors_name')

print('---------- During the last four quarters, how many hours did each employee work? ----------')
hours.select(values='QUARTER(hours_week_end) as Quarter, employees_name AS Employee, SUM(hours_qty) AS "Hours Worked"',
             joins=(employees,),
             group_by='QUARTER(hours_week_end), employees_name',
             order_by='QUARTER(hours_week_end)')

print('---------- Orders received later than expected ----------')
supplies.select(values=('suppliers_name as Supplier, supplies_description as Supply, '
                        'supply_orders_expected_date as Expected, supply_orders_received_date as Received'),
                joins=(supply_orders, suppliers),
                where='supply_orders_received_date > supply_orders_expected_date ')

print('---------- Supply orders not received yet ----------')
supplies.select(values=('suppliers_name as Supplier, supplies_description as Supply, '
                        'supply_orders_expected_date as Expected'),
                joins=(supply_orders, suppliers), where='supply_orders_received_date IS NULL')

print('---------- A month by month report of supply orders ----------')
supply_orders.select(
    values=('YEAR(supply_orders_expected_date) as Year, Month(supply_orders_expected_date) as Month, '
            'supplies_description as Supply'),
    joins=(supplies,),
    order_by='YEAR(supply_orders_expected_date)')
