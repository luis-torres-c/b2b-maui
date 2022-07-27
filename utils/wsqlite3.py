import os
import sqlite3

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.realpath(CURRENT_DIR)


class SQLiteManager:

    DATABASE_FILENAME = 'integrations.db'
    REALPATH_DATABASE = ROOT_DIR + os.sep + '..' + os.sep + DATABASE_FILENAME

    @classmethod
    def _database_path(cls):
        root_path = os.environ.get('SQLITE3_PATH') or cls.REALPATH_DATABASE
        db_path = root_path + os.sep + cls.DATABASE_FILENAME
        return db_path

    @classmethod
    def connection(cls):
        db = cls._database_path()
        return sqlite3.connect(db)

    @classmethod
    def initialize_db(cls):
        filepath = os.path.join(os.path.dirname(__file__), '..', 'sql', 'tables.sql')
        script = open(filepath, 'r').read()
        connection = sqlite3.connect(cls._database_path())
        connection.cursor().executescript(script)
        connection.commit()
