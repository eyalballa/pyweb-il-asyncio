import contextlib

import psycopg2


class DBConnection:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.db = None

    def connect(self):
        if self.db is None:
            self.db = psycopg2.connect(self.db_uri)

    def execute_and_get_first(self, query):
        with self.db.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()

    def execute_and_get_all(self, query):
        with self.db.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def close(self):
        if self.db is not None:
            self.db.close()


@contextlib.contextmanager
def db_connection(db_uri):
    db = DBConnection(db_uri)
    try:
        db.connect()
        yield db
    finally:
        db.close()


