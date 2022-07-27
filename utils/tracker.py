import calendar
import datetime

from utils.wsqlite3 import SQLiteManager


class Tracker(SQLiteManager):
    q_select = '''
        SELECT
            last_processed_day,
            last_flag
        FROM tracker
        WHERE name=?
    '''

    q_update = '''
        UPDATE tracker
        SET last_flag=?, last_processed_day=?
        WHERE name=?
    '''

    q_insert = '''
        INSERT INTO tracker
        ("name", "last_processed_day", "last_flag")
        VALUES
        (?, ?, 0)
    '''

    @classmethod
    def update_track(cls, name, last_processed_day, last_flag=0):
        conn = cls.connection()
        conn.execute(cls.q_update, (last_flag, last_processed_day, name))
        conn.commit()
        conn.close()

    @classmethod
    def select_track(cls, name):
        conn = cls.connection()
        result = conn.execute(cls.q_select, (name,)).fetchone()
        conn.close()
        return result

    @classmethod
    def create_track(cls, name, last_processed_day):
        conn = cls.connection()
        conn.execute(cls.q_insert, (name, last_processed_day))
        conn.commit()
        conn.close()


class DailyTracker(Tracker):
    @classmethod
    def create_track(cls, name, date):
        date_time = datetime.datetime.combine(date, datetime.time.max)
        ts = calendar.timegm(date_time.timetuple())
        Tracker.create_track(name, ts)
        return name, ts
