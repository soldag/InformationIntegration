import psycopg2
import logging


class DatabaseWriter:
    def __init__(self, host, port, database_name, schema, is_quiet=False):
        self.host = host
        self.port = port
        self.port = port
        self.database_name = database_name
        self.schema = schema
        self.is_quiet = is_quiet
        self.logger = logging.getLogger("databasewriter")

        self.isConnected = False
        self.connection = None
        self.cursor = None
        self.row_buffer = {}

    def connect(self, user, password):
        self.connection = psycopg2.connect(host=self.host,
                                           port=self.port,
                                           database=self.database_name,
                                           user=user,
                                           password=password)
        self.cursor = self.connection.cursor()
        self.isConnected = True

    def write_row(self, table_name, row):
        sql_template = self.row_to_sql(table_name, row)
        try:
            self.cursor.execute(sql_template, row)
            self.connection.commit()
        except (psycopg2.DataError, psycopg2.IntegrityError) as e:
            self.connection.rollback()
            self.logger.warning(e.message)

    @staticmethod
    def row_to_sql(table_name, row):
        column_names = ",".join(row.keys())
        value_placeholder = ",".join(map(lambda x: "%({})s".format(x), row.keys()))
        return "INSERT INTO {} ({}) VALUES ({});".format(table_name, column_names, value_placeholder)

    def create_tables(self):
        for table_name, columns in self.schema.iteritems():
            if not self.table_exists(table_name):
                column_definitions = ",".join(map(lambda (x, y): self.build_attr_definition(x, y), columns.items()))
                primary_key_statement = self.build_primary_key_statement(columns.items())
                self.cursor.execute("CREATE TABLE {} ({},{});".format(table_name, column_definitions, primary_key_statement))
        self.connection.commit()

    def drop_tables(self):
        table_names = ",".join(self.schema.keys())
        self.cursor.execute("DROP TABLE IF EXISTS {} CASCADE".format(table_names))
        self.connection.commit()

    @staticmethod
    def build_attr_definition(column_name, properties):
        definition = "{} {}".format(column_name, properties["type"])
        if "null" in properties and not properties["null"]:
            definition += " NOT NULL"

        return definition

    @staticmethod
    def build_primary_key_statement(column_definitions):
        keys = ",".join(map(lambda (x, y): x, filter(lambda (x, y): "primary_key" in y and y["primary_key"], column_definitions)))

        return "PRIMARY KEY({})".format(keys)

    def remove_violating_rows(self):
        if not self.is_quiet:
            print("Deleting rows violating foreign key constraints...")

        for table_name, columns in self.schema.iteritems():
            foreign_keys = map(lambda (x, y): (x, y["reference"]), filter(lambda (x, y): "reference" in y and y["reference"], columns.items()))
            for attribute, reference in foreign_keys:
                bracket_index = reference.index("(")
                referenced_table = reference[:bracket_index]
                referenced_attribute = reference[bracket_index+1:-1]
                self.cursor.execute("SELECT {} FROM {}".format(attribute, table_name))
                for row in self.cursor.fetchall():
                    if row[0] is not None:
                        self.cursor.execute("SELECT {0} FROM {1} WHERE {0} = %s".format(referenced_attribute, referenced_table), [row[0]])
                        if self.cursor.fetchone() is None:
                            if "primary_key" in columns[attribute] and columns[attribute]["primary_key"]:
                                self.cursor.execute("DELETE FROM {} WHERE {} = %s".format(table_name, attribute), [row[0]])
                            else:
                                self.cursor.execute("UPDATE {0} SET {1} = NULL WHERE {1} = %s".format(table_name, attribute), [row[0]])
                            self.connection.commit()

    def add_foreign_keys(self):
        if not self.is_quiet:
            print("Adding foreign key constraints...")

        for table_name, columns in self.schema.iteritems():
            foreign_keys = map(lambda (x, y): (x, y["reference"]), filter(lambda (x, y): "reference" in y and y["reference"], columns.items()))
            for attribute, reference in foreign_keys:
                try:
                    self.cursor.execute("ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}".format(table_name, attribute, reference))
                except psycopg2.IntegrityError as e:
                    self.connection.rollback()
                    self.logger.warning(e.message)
        self.connection.commit()

    def table_exists(self, table_name):
        self.cursor.execute("SELECT COUNT(relname) FROM pg_class WHERE relname = %s", [table_name])
        count = self.cursor.fetchone()

        return count[0] == 1

    def disconnect(self):
        self.isConnected = False
        self.cursor.close()
        self.connection.close()
