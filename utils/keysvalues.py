import json

from utils.wsqlite3 import SQLiteManager


class KeysValues(SQLiteManager):
    q_update = '''
        UPDATE keysvalues
        SET json=?
        WHERE name=?
    '''

    q_select = '''
        SELECT json
        FROM keysvalues
        WHERE name=?
    '''

    q_exists = '''
        SELECT 1
        FROM keysvalues
        WHERE name=?
    '''

    q_insert = '''
        INSERT INTO keysvalues(
        name, json)
        VALUES (?, ?)
    '''

    @classmethod
    def get_json(cls, name):
        conn = cls.connection()
        result = conn.execute(cls.q_select, (name,)).fetchone()
        conn.close()
        try:
            return json.loads(result[0])
        except TypeError:
            return None

    @classmethod
    def replace_json(cls, name, object_json):
        conn = cls.connection()
        conn.execute(cls.q_update, (json.dumps(object_json), name))
        conn.commit()
        conn.close()

    @classmethod
    def insert_or_update_json(cls, name, object_json):
        conn = cls.connection()
        exists = conn.execute(cls.q_exists, (name,)).fetchone()
        if exists:
            cls.replace_json(name, object_json)
        else:
            conn.execute(cls.q_insert, (name, json.dumps(object_json)))
            conn.commit()
        conn.close()
