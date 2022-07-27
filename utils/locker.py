from utils.wsqlite3 import SQLiteManager


class Locker(SQLiteManager):
    q_insert_lock = '''
        INSERT INTO locker
        (name, is_locked)
        VALUES (?, 1)
    '''

    q_remove_lock = '''
        DELETE FROM locker
        WHERE name=?
    '''

    q_select = '''
        SELECT is_locked
        FROM locker
        WHERE name=?
    '''

    @classmethod
    def _set_lock(cls, name):
        conn = cls.connection()
        conn.execute(cls.q_insert_lock, (name,))
        conn.commit()
        conn.close()

    @classmethod
    def _is_locked(cls, name):
        conn = cls.connection()
        result = conn.execute(cls.q_select, (name,)).fetchone()
        conn.close()
        return bool(result)

    @classmethod
    def _remove_lock(cls, name):
        conn = cls.connection()
        conn.execute(cls.q_remove_lock, (name,))
        conn.commit()
        conn.close()

    @classmethod
    def acquire(cls, name):
        if not cls._is_locked(name):
            cls._set_lock(name)
            return True
        return False

    @classmethod
    def release(cls, name):
        cls._remove_lock(name)
