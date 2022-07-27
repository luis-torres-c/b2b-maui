import datetime
import os
import csv
import time

from collections import defaultdict
from utils.logger import logger
import glob

class B2BBase:

    CATEGORIES = []

    WAIT_FOR_DATA = False

    HISTORICAL_CACHE_ENABLE = True

    date_format = '%d-%m-%Y'

    PORTAL = ''

    string_variables = {
    }

    def base_data(self, row, date_time):
        raise NotImplementedError

    def relation_values(self, base_data, relations, date_time, **kwargs):
        relation_list = [
            ['source', 'product'],
            ['source', 'store'],
            ['codproduct', 'product'],
            ['codstore', 'store'],
            ['category', 'product'],
            ['brand', 'product'],
            ['codmodel', 'product']
        ]

        for rel in relation_list:
            data1 = self.mapping_column_name(rel[0] + '_id')
            data2 = self.mapping_column_name(rel[1] + '_id')
            if data1 in base_data and data2 in base_data and base_data[data1] and base_data[data2]:
                rel_dict = {
                    data1: base_data[data1],
                    data2: base_data[data2]
                }
                relation_key = self.mapping_column_name(
                    rel[0]) + '->' + self.mapping_column_name(rel[1])
                self.append(relations[relation_key], date_time, rel_dict)

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):
        raise NotImplementedError

    def append(self, dict_, dt, record):
        string_date = dt.strftime('%Y%m%d')
        if string_date in dict_:
            dict_[string_date].append(record)
        else:
            dict_[string_date] = [record]

    def parse_data(self, connector, **kwargs):
        metrics = defaultdict(dict)
        relations = defaultdict(dict)

        data = connector.detalle_venta()

        for row in data:
            date_ = datetime.datetime.strptime(
                row['Datetime'], self.date_format).date()
            date_time = datetime.datetime.combine(date_, datetime.time.min)

            base_data = self.base_data(row, date_time)

            if not base_data:
                logger.debug('Skipping row {} from base_data'.format(row))
                continue

            self.relation_values(base_data, relations, date_time, **kwargs)

            self.metric_values(row, metrics, date_time, base_data, **kwargs)

        res = []
        if data:
            for metric_name, date_data_dict in metrics.items():
                for string_datetime, records in date_data_dict.items():
                    res.append({'metric': metric_name, 'date': datetime.datetime.strptime(
                        string_datetime, '%Y%m%d').date(), 'records': records})
            for relation_name, date_data_dict in relations.items():
                for string_datetime, records in date_data_dict.items():
                    no_duplicates = [
                        dict(t) for t in {
                            tuple(
                                d.items()) for d in records}]
                    res.append({'relation': relation_name, 'date': datetime.datetime.strptime(
                        string_datetime, '%Y%m%d').date(), 'records': no_duplicates})

        return res


class B2BWeb(B2BBase):

    string_variables = {
        'username': 'B2B_USERNAME',
        'password': 'B2B_PASSWORD'
    }

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        storage_path = os.environ['STORAGE_PATH']
        net_values = bool(int(os.environ['NET_VALUES']))
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'net_values': net_values,
        }


class B2BFile(B2BBase):
    string_variables = {
        'username': 'B2B_USERNAME',
    }

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        storage_path = os.environ['STORAGE_PATH']
        net_values = bool(int(os.environ['NET_VALUES']))
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'net_values': net_values,
        }


class B2BWebOC:

    HISTORICAL_CACHE_ENABLE = True

    date_format = '%Y-%m-%d'

    # dict whit key 'column name' and value 'date format from portal'
    COLUMNS_WHIT_DATE = {}

    PORTAL = ''

    string_variables = {
        'username': 'B2B_USERNAME',
        'password': 'B2B_PASSWORD'
    }

    BASE_COLUMN_NAMES = []
    PARSE_COLUMN_NAMES = {}

    DATA_NAME = 'oc'


    def base_data(self, row, date_time):
        raise NotImplementedError

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        storage_path = os.environ['STORAGE_PATH']
        source_int_path = os.environ['SOURCE_INT_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'source_int_path': source_int_path
        }

    def parse_data(self, connector, **kwargs):
        maui_hites_trakers = ['maui-oc-hites', 'maui-oc-hites-manual']
        if self.name in maui_hites_trakers:
            data = connector.detalle_venta(**kwargs)
        else:
            data = connector.detalle_venta()

        data_result = []

        for row in data:
            new_row = dict()
            for key in self.BASE_COLUMN_NAMES:
                new_row[key] = ''
            new_row['Cliente'] = self.PORTAL
            for key in self.PARSE_COLUMN_NAMES.keys():
                if isinstance(self.PARSE_COLUMN_NAMES[key], list):
                    for k in self.PARSE_COLUMN_NAMES[key]:
                        new_row[k] = row[key] if key in row else ''
                else:
                    new_row[self.PARSE_COLUMN_NAMES[key]
                            ] = row[key] if key in row else ''
            for key in new_row.keys():
                if key in self.COLUMNS_WHIT_DATE.keys():
                    new_row[key] = datetime.datetime.strptime(
                        new_row[key], self.COLUMNS_WHIT_DATE[key]).strftime(
                        self.date_format) if new_row[key] != '' else new_row[key]
            data_result.append(new_row)

        return [{
            'type': 'simple-data',
            'data_name': self.DATA_NAME,
            'records': data_result,
        }, ]

    def parse_data_manual(self, connector, **kwargs):
        maui_hites_trakers = ['maui-oc-hites', 'maui-oc-hites-manual']
        if self.name in maui_hites_trakers:
            data = connector.detalle_venta_manual(**kwargs)
        else:
            data = connector.detalle_venta_manual()

        data_result = []

        for row in data:
            new_row = dict()
            for key in self.BASE_COLUMN_NAMES:
                new_row[key] = ''
            new_row['Cliente'] = self.PORTAL
            for key in self.PARSE_COLUMN_NAMES.keys():
                if isinstance(self.PARSE_COLUMN_NAMES[key], list):
                    for k in self.PARSE_COLUMN_NAMES[key]:
                        new_row[k] = row[key] if key in row else ''
                else:
                    new_row[self.PARSE_COLUMN_NAMES[key]
                            ] = row[key] if key in row else ''
            for key in new_row.keys():
                if key in self.COLUMNS_WHIT_DATE.keys():
                    new_row[key] = datetime.datetime.strptime(
                        new_row[key], self.COLUMNS_WHIT_DATE[key]).strftime(
                        self.date_format) if new_row[key] != '' else new_row[key]
            data_result.append(new_row)

        return [{
            'type': 'simple-data',
            'data_name': self.DATA_NAME,
            'records': data_result,
        }, ]


class B2BPortalBase:

    # If portal has a restriction of days for requests periods,
    # False if portal can request all the period at once
    # Int value in days if portal has restricction.
    RESTRICTION_PERIOD = False

    # Raise error when not defined
    PORTAL = ''

    string_variables = {
        'username': 'B2B_USERNAME',
        'password': 'B2B_PASSWORD',
        'empresa': 'B2B_EMPRESA'
    }

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        source_int_path = os.environ['SOURCE_INT_PATH']
        return {
            'source_int_path': source_int_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        # TODO: Implement RESTRICTION PERIOD
        return connector.generate_files(**kwargs)
