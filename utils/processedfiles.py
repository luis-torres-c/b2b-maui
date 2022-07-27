from utils.wsqlite3 import SQLiteManager


class ProcessedFiles(SQLiteManager):

    q_insert = '''
    INSERT INTO processedfiles
    (file, checksum, name)
    VALUES (?, 0, ?)
    '''

    q_select = '''
    SELECT 1
    FROM processedfiles
    WHERE file=? and name=?
    '''

    @classmethod
    def set_processed_file(cls, file, name):
        conn = cls.connection()
        conn.execute(cls.q_insert, (file, name))
        conn.commit()
        conn.close()

    @classmethod
    def is_procesed_file(cls, file, name):
        conn = cls.connection()
        result = conn.execute(cls.q_select, (file, name)).fetchone()
        conn.close()
        return bool(result)
