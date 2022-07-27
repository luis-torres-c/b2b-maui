from collections import defaultdict

from utils.logger import logger


class Source:
    # TODO force somehow NotImplementError when CONNECTOR is None
    CONNECTOR = None

    MAPPING_COLUMN_NAMES = {}
    MAPPING_METRIC_NAMES = {}

    def mapping_metric_name(self, metric_name):
        if metric_name in self.MAPPING_METRIC_NAMES:
            return self.MAPPING_METRIC_NAMES[metric_name]
        return metric_name

    def mapping_column_name(self, col_name):
        if col_name in self.MAPPING_COLUMN_NAMES:
            return self.MAPPING_COLUMN_NAMES[col_name]
        return col_name

    def process(self, **kwargs):
        raise NotImplementedError

    def is_source_available(self, **kwargs):
        raise NotImplementedError


class FileSource(Source):

    DEFAULT_ENCODING = 'utf-8'

    APPEND = 'append'
    REPLACE = 'replace'
    DATA_MERGING_TYPE = APPEND

    def __merge_data(self, collection, new_data_lst):
        if self.APPEND == self.DATA_MERGING_TYPE:
            collection.extend(new_data_lst)
        elif self.REPLACE == self.DATA_MERGING_TYPE:
            # Using this option data_lst item must have date field as mandatory
            collection_by_date = defaultdict(list)
            for item in collection:
                key = '{}-{}'.format(item['metric'], item['date'])
                collection_by_date[key].append(item)

            for item in new_data_lst:
                key = '{}-{}'.format(item['metric'], item['date'])
                if key in collection_by_date:
                    # Deleting data to replace it for a new one.
                    del collection_by_date[key]
                collection_by_date[key].append(item)
            if collection_by_date:
                # Reset reference
                collection = []
            for items in collection_by_date.values():
                collection.extend(items)

        return collection

    def parse_data(self, object_files, **kwargs):
        raise NotImplementedError

    def files(self, connector, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        conn = self.CONNECTOR.get_instance(**kwargs)
        all_data = []
        files = self.files(conn, **kwargs)
        if not files:
            logger.warning('There are not files to process')
            # Calling to parse_data in case we need to control something else when files variable is empty
            self.parse_data(files, **kwargs)
        else:
            for files_tuple in files:
                files = []
                for file_path in files_tuple:
                    obj_file = conn.get_object_file(file_path, self.DEFAULT_ENCODING)
                    if obj_file.not_found:
                        logger.warning('File {} Not Found'.format(obj_file.filename))
                        continue
                    files.append(obj_file)
                logger.info('Starting data parsing for {}'.format(' '.join([o.filename for o in files])))
                all_data = self.__merge_data(all_data, self.parse_data(files, **kwargs))
                logger.info('Data Parsing Done for {}'.format(' '.join([o.filename for o in files])))

        return all_data


class DataBaseSource(Source):

    def parse_data(self, connector, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        conn = self.CONNECTOR.get_instance(**kwargs)
        logger.info('Starting data parsing...')
        return self.parse_data(conn)
