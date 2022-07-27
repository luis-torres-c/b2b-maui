from utils.wsqlite3 import SQLiteManager


class Counter(SQLiteManager):
    q_select = '''
        SELECT count
        FROM counter
        WHERE name=?
    '''

    q_update = '''
        UPDATE counter
        SET count=?
        WHERE name=?
    '''

    q_insert = '''
        INSERT INTO counter
        (name, count)
        VALUES
        (?, ?)
    '''

    @classmethod
    def set_count(cls, name, count):
        conn = cls.connection()
        res = conn.execute(cls.q_select, (name,)).fetchone()
        if res:
            conn.execute(cls.q_update, (count, name))
        else:
            conn.execute(cls.q_insert, (name, count))
        conn.commit()
        conn.close()

    @classmethod
    def get_count(cls, name):
        conn = cls.connection()
        res = conn.execute(cls.q_select, (name,))
        cursor = res.fetchone()
        if cursor:
            return cursor[0]
        else:
            return 0
        conn.close()
