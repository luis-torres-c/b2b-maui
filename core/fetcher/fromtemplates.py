import csv
import datetime
import os
import re

from collections import namedtuple
from collections import defaultdict

import xlrd
import inflection

from core.fetcher import DailyFetcher
from core.storages.base import GenericMetricsObjectsCSVStorage
from core.utils import datetime_to_wivo_format
from utils.logger import logger

Header = namedtuple('Headers', ['name', 'index'])
Object = namedtuple('Objects', ['id', 'index'])


def cell_to_date(book, cell):
    return datetime.datetime(*xlrd.xldate_as_tuple(cell.value, book.datemode)).date()


FOLDER_BASE = 'templates'


class GoalTemplate1:

    WEIGHTINGS_SHEET = 'ponderadores'

    WEIGHTINGS_WEEKDAYS = {
        0: 'Lunes',
        1: 'Martes',
        2: 'Miércoles',
        3: 'Jueves',
        4: 'Viernes',
        5: 'Sábado',
        6: 'Domingo',
    }

    STATIC_FIELD_GOAL_WEEK_NAME = 'Semana'
    STATIC_FIELD_GOAL_START_NAME = 'Inicio'
    STATIC_FIELD_GOAL_END_NAME = 'Fin'

    STATIC_FIELDS_GOALS = STATIC_FIELD_GOAL_WEEK_NAME + STATIC_FIELD_GOAL_START_NAME + STATIC_FIELD_GOAL_END_NAME

    GOALS_HEADER_PATTERN = '[Mm][Ee][Tt][Aa].*\((\w+:[\w\s]+)\)$'

    WEIGHTINGS_HEADER_PATTERN = '.*\((\w+)\)$'

    @classmethod
    def process(cls, file_path):

        book = xlrd.open_workbook(file_path)
        sheets = [book.sheet_by_index(index) for index in range(book.nsheets)]

        goals_sheets = [sh for sh in sheets if sh.name != cls.WEIGHTINGS_SHEET]
        weightings_sheet = [sh for sh in sheets if sh.name == cls.WEIGHTINGS_SHEET].pop()

        # Getting Headers info

        object_headers = []
        weightings_headers = []

        for nrow in range(weightings_sheet.nrows):
            if nrow == 0:
                # Skipping useless row
                continue
            elif nrow == 1:
                # Getting headers
                headers = weightings_sheet.row(nrow)
                for header_nrow in range(len(headers)):
                    header_name = headers[header_nrow]
                    object_header_match = re.match(cls.WEIGHTINGS_HEADER_PATTERN, header_name.value)
                    if object_header_match:
                        object_id_name = '{}_id'.format(inflection.singularize(object_header_match.group(1)))
                        object_headers.append(
                            Header(name=object_id_name, index=header_nrow))
                    elif header_name.value in cls.WEIGHTINGS_WEEKDAYS.values():
                        weightings_headers.append(Header(name=header_name.value, index=header_nrow))
            else:
                # break for loop
                break

        all_weightings_headers = object_headers + weightings_headers
        weightings_headers_indexes = {x.index: x.name for x in all_weightings_headers}

        weightings_info = []

        for nrow in range(weightings_sheet.nrows):
            if nrow in [0, 1]:
                # Useless rows
                continue

            row = weightings_sheet.row(nrow)
            new_row = {}
            for index, name in weightings_headers_indexes.items():
                cell = row[index]
                value = cell.value

                new_row[name] = value
            if any([True for val in new_row.values() if val == '']):
                logger.debug('Skipping {}'.format(row))
                continue

            weightings_info.append(new_row)

        # Getting meta info
        result_goals = []
        for goal_sheet in goals_sheets:
            result_in_days = defaultdict(list)
            header_info = {}
            metric_name = goal_sheet.name
            for nrow in range(goal_sheet.nrows):
                if nrow == 0:
                    headers = goal_sheet.row(nrow)
                    for header_index in range(len(headers)):
                        value = headers[header_index].value
                        m = re.match(cls.GOALS_HEADER_PATTERN, value)
                        if m:
                            value = m.group(1)
                        header_info[value] = header_index

                    continue

                row = goal_sheet.row(nrow)

                start_date = cell_to_date(book, row[header_info[cls.STATIC_FIELD_GOAL_START_NAME]])
                end_date = cell_to_date(book, row[header_info[cls.STATIC_FIELD_GOAL_END_NAME]])

                objects_from_goals = []
                for key, value in header_info.items():
                    if ':' in key:
                        pair = key.split(':')
                        index = value
                        object_id_name = '{}_id'.format(inflection.singularize(pair[0]))
                        object_id = pair[1]
                        objects_from_goals.append((index, object_id, object_id_name))

                while start_date <= end_date:
                    day_name = cls.WEIGHTINGS_WEEKDAYS[start_date.weekday()]
                    for wi in weightings_info:
                        object_data = {}
                        for object_header in object_headers:
                            object_data[object_header.name] = wi[object_header.name]

                        weighting_value = wi[day_name]
                        for index, object_id, object_id_name in objects_from_goals:
                            copied_object_data = object_data.copy()
                            base_data = {
                                object_id_name: object_id,
                                'datetime': datetime_to_wivo_format(start_date),
                                'value': format(row[index].value * weighting_value, '.2f'),
                            }
                            base_data.update(copied_object_data)
                            result_in_days[start_date].append(base_data)

                    start_date += datetime.timedelta(days=1)
            result_goals.append((metric_name, result_in_days))

        return result_goals


class GoalsGeneratorByTemplates(DailyFetcher):

    name = 'goals-generator-by-templates'

    WAIT_FOR_DATA = False

    FILE_PATTERN = '.*{date}.xlsx'

    pairs_folder_handler = [
        ('metas-plantilla-1', GoalTemplate1),
    ]

    @classmethod
    def settings(cls):
        source = os.environ['SOURCE_INT_PATH']
        storage_path = os.environ['STORAGE_PATH']
        return {
            'source': source,
            'storage_path': storage_path,
        }

    def process(self, **kwargs):
        source_path = kwargs['source']
        all_metas = []
        for folder, handler_class in self.pairs_folder_handler:
            files = os.listdir(os.path.join(source_path, FOLDER_BASE, folder))
            file_pattern = self.FILE_PATTERN.format(date=self.actual_date)
            files = [f for f in files if re.match(file_pattern, f)]
            if not files:
                logger.warning('Files not found for {}'.format(self.actual_date))
                continue

            full_file_path = os.path.join(source_path, FOLDER_BASE, folder, files.pop())

            all_metas.extend(handler_class.process(full_file_path))

        return all_metas

    def consolidate_data(self, data, **kwargs):
        storage_path = kwargs['storage_path']
        for metric_name, records_by_day in data:
            consolidated_date_by_month = defaultdict(list)
            for date, records in records_by_day.items():
                month = str(date.month).zfill(2)
                year = date.year
                key = '{}-{}'.format(year, month)
                consolidated_date_by_month[key].extend(records)

            for suffix, records in consolidated_date_by_month.items():
                filename = '{}_{}.csv'.format(metric_name, suffix)
                file_path = os.path.join(storage_path, 'metrics', metric_name, filename)
                logger.info('Saving file {}'.format(file_path))
                with open(file_path, 'w', encoding='utf-8') as output:
                    headers = records[0].keys()
                    writer = csv.DictWriter(output, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(records)


class SimpleMetricTemplate:

    STATIC_HEADERS = [
        'value'
    ]

    @classmethod
    def process(cls, file_path, actual_date):
        book = xlrd.open_workbook(file_path)
        sheets = [book.sheet_by_index(index) for index in range(book.nsheets)]

        result = []

        datetime_wivo = datetime_to_wivo_format(actual_date)

        for sheet in sheets:
            metric_name = inflection.pluralize(sheet.name)
            records = []
            header_info = {}
            for nrow in range(sheet.nrows):
                if nrow == 0:
                    headers = sheet.row(nrow)
                    for header_nrow in range(len(headers)):
                        value = headers[header_nrow].value
                        if value in cls.STATIC_HEADERS:
                            header_info[header_nrow] = value
                        else:
                            header_info[header_nrow] = '{}_id'.format(inflection.singularize(value))
                else:
                    row = sheet.row(nrow)
                    record = {}
                    for i in range(len(row)):
                        if header_info[i] in cls.STATIC_HEADERS or row[i].ctype == xlrd.XL_CELL_TEXT:
                            record[header_info[i]] = row[i].value
                        elif row[i].ctype == xlrd.XL_CELL_NUMBER:
                            record[header_info[i]] = str(int(row[i].value))
                        else:
                            record[header_info[i]] = str(row[i].value)

                    record.update({'datetime': datetime_wivo})
                    records.append(record)

            result.append({'metric': metric_name, 'records': records})

        return result


class CloneGenerator(GenericMetricsObjectsCSVStorage, DailyFetcher):

    name = 'clone-metrics-by-templates'

    HISTORICAL_CACHE_ENABLE = True
    WAIT_FOR_DATA = False

    FOLDER = 'clones-plantilla-1'
    FILE_PATTERN = '.*{date}.xlsx?'
    YEAR_LIMIT = 2015

    @classmethod
    def settings(cls):
        source = os.environ['SOURCE_INT_PATH']
        storage_path = os.environ['STORAGE_PATH']
        return {
            'source': source,
            'storage_path': storage_path,
        }

    def process(self, **kwargs):
        source_path = kwargs['source']
        files = os.listdir(os.path.join(source_path, FOLDER_BASE, self.FOLDER))
        date_pointer = self.actual_date
        while date_pointer.year != self.YEAR_LIMIT:
            logger.debug('Looking for template for {}'.format(date_pointer))
            file_pattern = self.FILE_PATTERN.format(date=date_pointer)
            found_files = [f for f in files if re.match(file_pattern, f)]
            if found_files:
                break
            date_pointer -= datetime.timedelta(days=1)

        if date_pointer.year == self.YEAR_LIMIT:
            logger.warning('Not Found Template for actual date {}'.format(self.actual_date))
            return []

        result = []
        for fn in found_files:
            file_path = os.path.join(source_path, FOLDER_BASE, self.FOLDER, fn)
            logger.info('Processing File {}'.format(file_path))
            result.extend(SimpleMetricTemplate.process(file_path, self.actual_date))

        return result
