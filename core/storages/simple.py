import copy
import csv
import os

from core.storages.base import Storage
from utils.logger import logger
from utils.wmongo import MongoDbManager


class _SimpleHistoricalStorageMixIn:
    @classmethod
    def consolidate_data_on_db(cls, data_name, simple_data, group_data_id, source):

        _HistoricalData.delete_simple_data(data_name, group_data_id, source)
        _HistoricalData.save_simple_data(data_name, simple_data['records'], group_data_id, source)
        all_simple_data = _HistoricalData.get_simple_data(data_name, group_data_id)
        return all_simple_data


class _SimpleCsvStorageMixIn:

    FILENAME_TEMPLATE = '{main_filename}_{suffix}.csv'
    main_filename = ''
    file_folder = ''
    DELIMITER = ','

    @classmethod
    def create_file(cls, data_name, simple_data_records, suffix):
        if not simple_data_records:
            return
        root_path = os.environ['SOURCE_PATH']
        fieldnames = simple_data_records[0].keys()
        main_filename = cls.main_filename or data_name
        filename = cls.FILENAME_TEMPLATE.format(main_filename=main_filename, suffix=suffix)
        output_filepath = os.path.join(root_path, cls.file_folder, filename)
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, 'w', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=cls.DELIMITER)
            writer.writeheader()
            writer.writerows(simple_data_records)

        logger.info('Saved File {}'.format(output_filepath))


class SimpleDailyCsvStorage(_SimpleCsvStorageMixIn, _SimpleHistoricalStorageMixIn, Storage):

    def _normalize_simple_data(self, simple_data):
        for item in simple_data:
            if 'representative_date' not in item:
                item['representative_date'] = str(self.actual_date_tz or self.actual_date)
            else:
                item['representative_date'] = str(item['representative_date'])

    def consolidate_data(self, simple_data, **kwargs):
        if not simple_data:
            return

        self._normalize_simple_data(simple_data)

        for item in simple_data:
            group_data_id = item['representative_date']
            source = self.name
            data_name = item['data_name']

            all_simple_data_records = self.consolidate_data_on_db(data_name, item, group_data_id, source)
            self.create_file(data_name, all_simple_data_records, group_data_id)


class _HistoricalData(MongoDbManager):
    DATABASE_NAME = 'historical_simple_daily_data'

    @classmethod
    def save_simple_data(cls, data_name, simple_data_records, group_data_id, source):
        db = cls.connection(cls.DATABASE_NAME)
        collection = db[data_name]
        copied_records = []
        for dict_row in simple_data_records:
            copied_rec = copy.copy(dict_row)
            copied_rec.update({
                'group_data_id': group_data_id,
                'source': source,
            })
            copied_records.append(copied_rec)

        # TODO log the result or catch exception if applied
        if not simple_data_records:
            return
        collection.insert_many(copied_records)

    @classmethod
    def delete_simple_data(cls, data_name, group_data_id, source):
        db = cls.connection(cls.DATABASE_NAME)
        collection = db[data_name]
        # TODO log the result or catch exception if applied
        collection.delete_many(
            {
                'group_data_id': group_data_id,
                'source': source,
            }
        )

    @classmethod
    def get_simple_data(cls, data_name, group_data_id):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'group_data_id' in copied_data:
                del copied_data['group_data_id']
            if 'source' in copied_data:
                del copied_data['source']
            return copied_data

        new_simple_data = []
        db = cls.connection(cls.DATABASE_NAME)
        collection = db[data_name]
        cursor = collection.find({'group_data_id': group_data_id})
        for index, record in enumerate(cursor, start=1):
            new_simple_data.append(convert(record))

        return new_simple_data
