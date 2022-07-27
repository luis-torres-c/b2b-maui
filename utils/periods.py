from datetime import datetime, timedelta

from utils.wsqlite3 import SQLiteManager


class Period(SQLiteManager):
    q_insert_period = '''
        INSERT INTO periods
        (tag, date_from, date_to, status)
        VALUES (?, ?, ?, ?)
    '''

    q_remove_periods = '''
        DELETE FROM periods
        WHERE tag=?
    '''

    q_remove_period = '''
        DELETE FROM periods
        WHERE tag=?
        AND date_to=?
        AND date_from=?
        AND status=?
    '''

    q_select_all_periods = '''
        SELECT *
        FROM periods
        WHERE tag=?
    '''

    q_select_by_status = '''
        SELECT *
        FROM periods
        WHERE tag=? AND status=?
    '''

    q_select_not_processed = '''
        SELECT *
        FROM periods
        WHERE tag=? AND status == "empty"
    '''

    q_select_ready = '''
        SELECT *
        FROM periods
        WHERE tag=? AND status == "ready"
    '''

    def _normalize(cls, tag):
        conn = cls.connection()
        results = conn.execute(cls.q_select_all_periods, (tag, ))
        results = results.fetchall()

        to_insert = []
        to_delete = []
        for period1 in results:
            for period2 in results:
                if period1[1] == period2[2] and period1[3] == period2[3]:
                    to_delete.append(period1)
                    to_delete.append(period2)
                    new_period = (period1[0], period1[2], period2[1], period1[3])
                    if new_period not in to_insert:
                        to_insert.append(new_period)
        for insert in to_insert:
            conn.execute(cls.q_insert_period, insert)
        for delete in to_delete:
            conn.execute(cls.q_remove_period, delete)
        conn.commit()
        conn.close()

    @classmethod
    def insert_ready_period(cls, tag, date_from, date_to, status):
        conn = cls.connection()
        conn.execute(cls.q_insert_period, (tag, date_from, date_to, status))
        conn.commit()
        conn.close()

    @classmethod
    def insert_period(cls, tag, date_from, date_to, status):
        conn = cls.connection()
        results = conn.execute(cls.q_select_by_status, (tag, 'ok'))
        results = results.fetchall()

        dict_status = dict()
        for period in results:
            start_date = datetime.strptime(period[2], '%Y-%m-%d')
            final_date = datetime.strptime(period[1], '%Y-%m-%d')
            while start_date <= final_date:
                dict_status[start_date] = period[3]
                start_date += timedelta(days=1)

        results = conn.execute(cls.q_select_not_processed, (tag, ))
        results = results.fetchall()

        for period in results:
            start_date = datetime.strptime(period[2], '%Y-%m-%d')
            final_date = datetime.strptime(period[1], '%Y-%m-%d')
            while start_date <= final_date:
                dict_status[start_date] = period[3]
                start_date += timedelta(days=1)

        start_date = datetime.strptime(date_from, '%Y-%m-%d')
        final_date = datetime.strptime(date_to, '%Y-%m-%d')
        while start_date <= final_date:
            dict_status[start_date] = status
            start_date += timedelta(days=1)

        date_list = list(dict_status.keys())
        date_list.sort()
        list_to_insert = list()
        aux_from = None
        aux_to = None
        aux_status = None
        for date in date_list:
            if not aux_from:
                aux_from = date
            if not aux_status:
                aux_status = dict_status[date]
            if not aux_to:
                aux_to = date
            if aux_status == dict_status[date] and (date - aux_to).days <= 1:
                aux_to = date
            else:
                list_to_insert.append([aux_from, aux_to, aux_status])
                aux_from = date
                aux_to = date
                aux_status = dict_status[date]
        list_to_insert.append([aux_from, aux_to, aux_status])

        conn.execute(cls.q_remove_periods, (tag, ))
        for per in list_to_insert:
            conn.execute(cls.q_insert_period, (tag, per[0].strftime('%Y-%m-%d'), per[1].strftime('%Y-%m-%d'), per[2]))
        conn.commit()
        conn.close()

    @classmethod
    def remove_periods(cls, tag):
        conn = cls.connection()
        conn.execute(cls.q_select, (tag,))
        conn.close()

    @classmethod
    def select_all_periods(cls, tag):
        conn = cls.connection()
        results = conn.execute(cls.q_select_all_periods, (tag,))
        dicts_results = []
        for result in results:
            dicts_results.append({
                'tag': result[0],
                'from': result[2],
                'to': result[1],
                'status': result[3],
            })
        conn.close()
        return dicts_results

    @classmethod
    def select_by_status(cls, tag, status):
        conn = cls.connections()
        results = conn.execute(cls.q_select_by_status, (tag, status))
        conn.close()
        # TODO: Profit return data (maybe a list of dict)
        return results

    @classmethod
    def select_not_processed(cls, tag):
        conn = cls.connection()
        results = conn.execute(cls.q_select_not_processed, (tag,))
        dicts_results = []
        for result in results:
            dicts_results.append({
                'tag': result[0],
                'from': result[2],
                'to': result[1],
                'status': result[3],
            })
        conn.close()
        return dicts_results

    @classmethod
    def select_ready_to_process(cls, tag):
        conn = cls.connection()
        results = conn.execute(cls.q_select_ready, (tag,))
        dicts_results = []
        for result in results:
            dicts_results.append({
                'tag': result[0],
                'from': result[2],
                'to': result[1],
                'status': result[3],
            })
        conn.close()
        return dicts_results

    @classmethod
    def last_date_tagged(cls, tag):
        conn = cls.connection()
        results = conn.execute(cls.q_select_all_periods, (tag,))
        last = '2020-01-01'
        date_last = datetime.strptime(last, "%Y-%m-%d")
        for period in results:
            date_period = datetime.strptime(period[1], "%Y-%m-%d")
            if date_period > date_last:
                last = period[1]
        conn.close()
        return last
