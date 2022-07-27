import csv
import copy
import os
import re

import inflection

from utils.wmongo import MongoDbManager
from utils.logger import logger


class Storage:
    def consolidate_data(self, data, **kwargs):
        raise NotImplementedError


class RelationCollection:
    def __init__(self, relations):
        self.all_data = {}
        self._normalize(relations)

    def _normalize(self, relations):
        for parsed_data in relations:
            if not parsed_data['records']:
                logger.debug('Skipping Empty Relation {}'.format(parsed_data))
                continue

            relation_name = parsed_data['relation']
            date = parsed_data['date']
            key = '{}-{}'.format(relation_name, date)
            if key in self.all_data:
                self.all_data[key].extend_records(parsed_data['records'])
            else:
                rel = Relation(relation_name, parsed_data['records'], date)
                self.all_data[key] = rel

    @property
    def relation_names(self):
        names = set()
        for rel in self.all_data.values():
            names.add(rel.name)
        return names

    def items(self):
        return list(self.all_data.values())


class MetricCollection:

    def __init__(self, metrics):
        self.all_data = {}
        self._normalize(metrics)

    def _normalize(self, metrics):
        for parsed_data in metrics:
            if not parsed_data['records']:
                logger.debug('Skipping Empty Metric {}'.format(parsed_data))
                continue
            metric_name = parsed_data['metric']
            date = parsed_data['date']
            self.add_metric_records(metric_name, date, parsed_data['records'])

    def add_metric_records(self, metric_name, date, records):
        key = '{}-{}'.format(metric_name, date)
        if key in self.all_data:
            self.all_data[key].extend_records(records)
        else:
            instance = Metric(
                metric_name,
                records,
                date,
            )
            self.all_data[key] = instance

    @property
    def metric_names(self):
        names = set()
        for metric in self.all_data.values():
            names.add(metric.name)
        return names

    def items(self):
        return list(self.all_data.values())

    def get_object_collection(self):
        data_objects = []
        # FIXME improve pattern bon, this pattern is not supported <something>_<something>_id
        pattern_bon = '{}_'
        for metric in self.items():
            for record in metric.records:
                for bon in metric.base_object_names:
                    pattern = pattern_bon.format(bon)
                    one_record = {k: record[k] for k in record.keys() if k.startswith(pattern)}
                    if one_record[bon + '_id']:
                        data_objects.append(
                            {
                                'object_name': bon,
                                'record': one_record,
                                'date': metric.date,
                            }
                        )

        return ObjectCollection(data_objects)


class ObjectCollection:

    def __init__(self, objects):
        self.all_data = {}
        self._normalize(objects)

    @classmethod
    def _get_object_id_key(cls, fieldnames):
        for field in fieldnames:
            m = Object.COLUMN_OBJECT_ID_PATTERN.match(field)
            if m:
                return field
        logger.error('There is not a column as field ID')

    def _normalize(self, objects):
        for dict_obj in objects:
            object_name = dict_obj['object_name']
            date = dict_obj.get('date') or ''
            key = '{}-{}'.format(object_name, date)
            if key in self.all_data:
                self.all_data[key].add_record(dict_obj['record'])
            else:
                obj = Object(object_name, dict_obj['record'].keys(), date)
                obj.add_record(dict_obj['record'])
                self.all_data[key] = obj

        return self.all_data

    @property
    def object_names(self):
        names = set()
        for obj in self.all_data.values():
            names.add(obj.name)
        return names

    def items(self):
        return list(self.all_data.values())


class Metric:

    OPTIONAL_METRIC_FIELDNAMES = [
        'ticket_id',
    ]

    REQUIRED_METRIC_FIELDNAMES = [
        'datetime',
        'value',
    ]

    NON_OBJECT_FIELDNAMES = OPTIONAL_METRIC_FIELDNAMES + REQUIRED_METRIC_FIELDNAMES

    REQUIRED_OBJECT_FIELDNAMES = [
        'object_id',
        'name',
    ]

    COLUMN_FIELDNAMES_PATTERN = re.compile('(\w+)_(.+)')

    def __init__(self, name, records, date):
        self.name = name
        self.pluralized_name = inflection.pluralize(name)
        self.records = records
        self.date = date
        self.string_date = date.strftime('%Y-%m-%d')

    @property
    def fieldnames(self):
        return self.records[0].keys()

    def extend_records(self, records):
        self.records.extend(records)

    def add_record(self, record):
        self.records.append(record)

    @property
    def metric_fieldnames(self):
        base_object_names = self.base_object_names
        return self.REQUIRED_METRIC_FIELDNAMES + \
            [''.join([field, '_id']) for field in base_object_names] + \
            [field for field in self.OPTIONAL_METRIC_FIELDNAMES if field in self.fieldnames]

    @property
    def base_object_names(self):
        object_fieldnames = list(filter(
            lambda fieldname: fieldname not in self.NON_OBJECT_FIELDNAMES,
            self.fieldnames
        ))
        return set([self.COLUMN_FIELDNAMES_PATTERN.match(field).group(1) for field in object_fieldnames])

    def clean_records(self):
        self.records = []


class Object:

    COLUMN_OBJECT_ID_PATTERN = re.compile(r'(\w+)_id')
    COLUMN_OBJECT_NAME_PATTERN = re.compile(r'(\w+)_name')

    def __init__(self, name, fieldnames, date):
        self.name = name
        self.pluralized_name = inflection.pluralize(name)
        self.records = []
        self.records_with_original_names = []
        self.date = date
        self.string_date = date.strftime('%Y-%m-%d')
        self.initial_fieldnames = fieldnames
        self._map_fieldnames = [self._map_field_name(k) for k in fieldnames]
        self.fieldnames = [t[0] for t in self._map_fieldnames]
        self._unique_ids = set()
        self.is_valid = len(self.initial_fieldnames) > 1

    def _map_field_name(self, fieldname):
        required_field_id = list(filter(lambda field: field.endswith('_id'), Metric.REQUIRED_OBJECT_FIELDNAMES))[0]
        required_field_description = list(filter(
            lambda field: field.endswith('name'), Metric.REQUIRED_OBJECT_FIELDNAMES))[0]
        m = self.COLUMN_OBJECT_ID_PATTERN.match(fieldname)
        if m:
            return required_field_id, fieldname
        m = self.COLUMN_OBJECT_NAME_PATTERN.match(fieldname)
        if m:
            return required_field_description, fieldname
        if fieldname.startswith(self.name):
            return fieldname.replace('{}_'.format(self.name), ''), fieldname
        return fieldname.split('_')[1], fieldname

    def add_record(self, record):
        object_key = self.id_field_name
        object_id = record[object_key]
        if object_id in self._unique_ids:
            return

        self._unique_ids.add(object_id)
        new_record = {t[0]: record[t[1]] for t in self._map_fieldnames}

        self.records_with_original_names.append(record)
        self.records.append(new_record)

    @property
    def id_field_name(self):
        for field in self.initial_fieldnames:
            m = self.COLUMN_OBJECT_ID_PATTERN.match(field)
            if m:
                return field
        logger.error('There is not a column as field ID')

    def clean_records(self):
        self.records = []
        self.records_with_original_names = []
        self._unique_ids = set()


class Relation:
    def __init__(self, name, records, date):

        self.date = date
        self.string_date = date.strftime('%Y-%m-%d')
        self.name, self.parent_name, self.child_name = self._parse_name(name)
        self.parent_name_id = '{}_id'.format(inflection.singularize(self.parent_name))
        self.child_name_id = '{}_id'.format(inflection.singularize(self.child_name))
        self.records = []
        self.records_with_original_names = []
        self._unique_ids = set()
        self._adds_and_normalizes_records(records)

    def _parse_name(self, name):
        parent, child = name.split('->')
        parent = inflection.pluralize(parent)
        child = inflection.pluralize(child)
        relation_name = '{}_{}'.format(parent, child)
        return relation_name, parent, child

    def _adds_and_normalizes_records(self, records):
        new_records = []
        for record in records:
            rel_id = '{}-{}'.format(record[self.parent_name_id], record[self.child_name_id])
            if rel_id in self._unique_ids:
                continue

            new_record = copy.deepcopy(record.copy())
            new_record[self.id_field_name] = rel_id
            new_records.append(new_record)

        self.records.extend(new_records)
        self.records_with_original_names.extend(records)

    def extend_records(self, records):
        self._adds_and_normalizes_records(records)

    def add_record(self, record):
        self._adds_and_normalizes_records([record])

    def clean_records(self):
        self.records = []
        self.records_with_original_names = []
        self._unique_ids = set()

    @property
    def id_field_name(self):
        return 'relation_id'

    @property
    def fieldnames(self):
        return [self.parent_name_id, self.child_name_id]

    @property
    def is_valid(self):
        if self.records_with_original_names:
            return True
        return False


class GenericMetricsObjectsCSVStorage(Storage):

    CREATION_OBJECTS = True

    HISTORICAL_CACHE_ENABLE = False

    HISTORICAL_METRIC_ENABLE = True

    STORAGE_LOGIC_WITH_INDEXES = bool(int(os.environ.get('STORAGE_LOGIC_WITH_INDEXES', 0)))

    def _create_relation(self, root_path, relation):

        suffix = relation.string_date

        prefix = '{}->{}'.format(relation.parent_name, relation.child_name)

        output_relation_file = '{}/relations/{}-{}.csv'.format(
            root_path, prefix, suffix)
        os.makedirs(os.path.dirname(output_relation_file), exist_ok=True)
        logger.debug('Saving Relation {}'.format(output_relation_file))
        with open(output_relation_file, 'w', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=relation.fieldnames)
            writer.writeheader()
            for record in relation.records_with_original_names:
                writer.writerow(record)

    def _create_object(self, root_path, object):
        # FIXME improve this validation, when an object doesnt have the minimum required fields
        if len(object.fieldnames) < 2:
            logger.debug('Skipping Object Creation for {}'.format(object.name))
            return

        suffix = object.string_date

        pluralized_object_name = object.pluralized_name
        output_object_file = '{}/objects/{}/{}_{}.csv'.format(
            root_path, pluralized_object_name, pluralized_object_name, suffix)
        os.makedirs(os.path.dirname(output_object_file), exist_ok=True)
        logger.debug('Saving Object {}'.format(output_object_file))
        with open(output_object_file, 'w', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=object.fieldnames)
            writer.writeheader()
            for record in object.records:
                writer.writerow(record)

    def _consolidate_metrics(self, metrics, **kwargs):

        if not metrics:
            return

        def update_dates(data, date):
            for d in data:
                if not ('date' in d and d.get('date')):
                    d['date'] = date

        storage_path = kwargs['storage_path']

        generation_date = self.actual_date_tz or self.actual_date

        # Updating dates for those metrics that don't have a defined date.
        # using actual date.
        update_dates(metrics, generation_date)

        metric_collection = MetricCollection(metrics)
        object_collection = metric_collection.get_object_collection()

        if self.STORAGE_LOGIC_WITH_INDEXES:

            if self.HISTORICAL_CACHE_ENABLE:
                # Creating Indexes if doesnt exist
                HistoricalData.create_indexes_for_objects(object_collection)
                # Deleting previous data for this range time (generation date)
                if self.HISTORICAL_METRIC_ENABLE:
                    # Creating Indexes if doesnt exist
                    HistoricalData.create_indexes_for_metrics(metric_collection)
                    HistoricalData.delete_metrics(metric_collection.metric_names, generation_date, self.name)
                HistoricalData.delete_objects(object_collection.object_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_metric_collection = MetricCollection(copy.deepcopy(metrics))
                new_object_collection = metric_collection.get_object_collection()
                HistoricalData.save_objects(new_object_collection, generation_date, self.name)
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.save_metrics(new_metric_collection, generation_date, self.name)

                # Adding complementary data from database.
                HistoricalData.get_objects(object_collection)
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.get_metrics(metric_collection)

            for metric in metric_collection.items():
                records = metric.records
                if not metric.records:
                    continue

                pluralized_metric_name = metric.pluralized_name
                metric_fieldnames = metric.metric_fieldnames

                output_metric_file = '{}/metrics/{}/{}_{}.csv'.format(
                    storage_path, pluralized_metric_name, pluralized_metric_name, metric.string_date)
                os.makedirs(os.path.dirname(output_metric_file), exist_ok=True)
                logger.debug('Saving Metric {}'.format(output_metric_file))
                with open(output_metric_file, 'w', encoding='utf-8') as output_csv_metric:
                    writer = csv.DictWriter(output_csv_metric, fieldnames=metric_fieldnames)
                    writer.writeheader()
                    for record in records:
                        writer.writerow({key: record[key] for key in metric_fieldnames})

            for obj in object_collection.items():
                self._create_object(
                    storage_path,
                    obj,
                )
        else:
            if self.HISTORICAL_CACHE_ENABLE:
                # Deleting previous data for this range time (generation date)
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.delete_metrics(metric_collection.metric_names, generation_date, self.name)
                HistoricalData.delete_objects(object_collection.object_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_metric_collection = MetricCollection(copy.deepcopy(metrics))
                new_object_collection = metric_collection.get_object_collection()
                # Adding complementary data from database.
                HistoricalData.merge_objects(object_collection)
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.merge_metrics(metric_collection)

            for metric in metric_collection.items():
                records = metric.records
                if not metric.records:
                    continue

                pluralized_metric_name = metric.pluralized_name
                metric_fieldnames = metric.metric_fieldnames

                output_metric_file = '{}/metrics/{}/{}_{}.csv'.format(
                    storage_path, pluralized_metric_name, pluralized_metric_name, metric.string_date)
                os.makedirs(os.path.dirname(output_metric_file), exist_ok=True)
                logger.debug('Saving Metric {}'.format(output_metric_file))
                with open(output_metric_file, 'w', encoding='utf-8') as output_csv_metric:
                    writer = csv.DictWriter(output_csv_metric, fieldnames=metric_fieldnames)
                    writer.writeheader()
                    for record in records:
                        writer.writerow({key: record[key] for key in metric_fieldnames})

            for obj in object_collection.items():
                self._create_object(
                    storage_path,
                    obj,
                )

            if self.HISTORICAL_CACHE_ENABLE:
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.save_metrics(new_metric_collection, generation_date, self.name)
                HistoricalData.save_objects(new_object_collection, generation_date, self.name)

    def _consolidate_objects(self, objects, **kwargs):

        if not objects:
            return

        def update_dates(data, date):
            for d in data:
                if not ('date' in d and d.get('date')):
                    d['date'] = date

        def convert_to_new_structure(data):
            new_list = []
            for d in data:
                object_name = d['object']
                date = d['date']
                for record in d['records']:
                    new_list.append({
                        'object_name': inflection.pluralize(object_name),
                        'date': date,
                        'record': record,
                    })
            return new_list

        def delete_empty_objects(data):
            new_list = []
            for d in data:
                singularized_object_name = inflection.singularize(d['object_name'])
                if d['record'][singularized_object_name + '_id']:
                    new_list.append(d)
            return new_list

        generation_date = self.actual_date_tz or self.actual_date

        update_dates(objects, generation_date)
        # FIXME this is for legacy behavior, change it in the near future
        new_objects = convert_to_new_structure(objects)

        new_objects = delete_empty_objects(new_objects)

        storage_path = kwargs['storage_path']

        object_collection = ObjectCollection(new_objects)

        if self.STORAGE_LOGIC_WITH_INDEXES:

            if self.HISTORICAL_CACHE_ENABLE:
                # Creating Indexes if doesnt exist
                HistoricalData.create_indexes_for_objects(object_collection)
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_objects(object_collection.object_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_object_collection = ObjectCollection(copy.deepcopy(new_objects))
                HistoricalData.save_objects(new_object_collection, generation_date, self.name)

                # Adding complementary data from database.
                HistoricalData.get_objects(object_collection)

            for obj in object_collection.items():
                self._create_object(
                    storage_path,
                    obj,
                )
        else:
            if self.HISTORICAL_CACHE_ENABLE:
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_objects(object_collection.object_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_object_collection = ObjectCollection(copy.deepcopy(new_objects))
                # Adding complementary data from database.
                HistoricalData.merge_objects(object_collection)

            for obj in object_collection.items():
                self._create_object(
                    storage_path,
                    obj,
                )

            if self.HISTORICAL_CACHE_ENABLE:
                HistoricalData.save_objects(new_object_collection, generation_date, self.name)

    def _consolidate_relations(self, relations, **kwargs):

        if not relations:
            return

        def update_dates(data, date):
            for d in data:
                if not ('date' in d and d.get('date')):
                    d['date'] = date

        generation_date = self.actual_date_tz or self.actual_date

        update_dates(relations, generation_date)

        relation_collection = RelationCollection(relations)

        if self.STORAGE_LOGIC_WITH_INDEXES:
            if self.HISTORICAL_CACHE_ENABLE:
                # Creating Indexes if doesnt exist
                HistoricalData.create_indexes_for_relations(relation_collection)
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_relations(relation_collection.relation_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_relation_collection = RelationCollection(copy.deepcopy(relations))
                HistoricalData.save_relations(new_relation_collection, generation_date, self.name)

                # Adding complementary data from database.
                HistoricalData.get_relations(relation_collection)

            storage_path = kwargs['storage_path']
            for relation in relation_collection.items():
                self._create_relation(storage_path, relation)
        else:
            if self.HISTORICAL_CACHE_ENABLE:
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_relations(relation_collection.relation_names, generation_date, self.name)
                # copied collections from original generation to be saved in database.
                new_relation_collection = RelationCollection(copy.deepcopy(relations))
                # Adding complementary data from database.
                HistoricalData.merge_relations(relation_collection)

            storage_path = kwargs['storage_path']
            for relation in relation_collection.items():
                self._create_relation(storage_path, relation)

            if self.HISTORICAL_CACHE_ENABLE:
                HistoricalData.save_relations(new_relation_collection, generation_date, self.name)

    def consolidate_data(self, data, **kwargs):

        metrics_data = []
        objects_data = []
        relations_data = []
        for item in data:
            if 'metric' in item:
                metrics_data.append(item)
            elif 'object' in item:
                objects_data.append(item)
            elif 'relation' in item:
                relations_data.append(item)

        self._consolidate_metrics(metrics_data, **kwargs)
        self._consolidate_objects(objects_data, **kwargs)
        self._consolidate_relations(relations_data, **kwargs)


class HistoricalData(MongoDbManager):
    METRIC_DATABASE_NAME = 'historical_metrics'
    OBJECT_DATABASE_NAME = 'historical_objects'
    RELATIONS_DATABASE_NAME = 'historical_relations'

    @classmethod
    def create_db_indexes(cls, database_name, collection_names):
        db = cls.connection(database_name)
        for col_name in collection_names:
            collection = db[col_name]
            index_names = [i['name'] for i in collection.list_indexes()]
            # Checking if already exists
            if not any([True for i in index_names if re.search(r'^date_\d$', i)]):
                logger.debug('Creating Index DATE for Collection: {} Database: {}'.format(col_name, database_name))
                collection.create_index([
                    ('date', cls.ASCENDING)])
                logger.debug('Created DATE index')
            else:
                logger.debug('Index DATE already exists for Collection: {} Database: {}'.format(col_name, database_name))
            # Checking if already exists
            if not any([True for i in index_names if re.search(r'generation_date_\d_source_\d', i)]):
                logger.debug('Creating Index GENERATION_DATA/SOURCE for {} collection {} database'.format(
                    col_name, database_name))
                collection.create_index([
                    ('generation_date', cls.ASCENDING),
                    ('source', cls.ASCENDING)]
                )
                logger.debug('Created GENERATION_DATE/SOURCE index')
            else:
                logger.debug(
                    'Indexes GENERATION_DATA and SOURCE already exist for {} collection {} databse'.format(
                        col_name, database_name))

    @classmethod
    def create_indexes_for_metrics(cls, metric_collection):
        cls.create_db_indexes(cls.METRIC_DATABASE_NAME, metric_collection.metric_names)

    @classmethod
    def create_indexes_for_objects(cls, object_collection):
        cls.create_db_indexes(cls.OBJECT_DATABASE_NAME, object_collection.object_names)

    @classmethod
    def create_indexes_for_relations(cls, relation_collection):
        cls.create_db_indexes(cls.RELATIONS_DATABASE_NAME, relation_collection.relation_names)

    @classmethod
    def delete_collections(cls, database_name, collection_names, generation_date, source):
        db = cls.connection(database_name)
        for name in collection_names:
            collection = db[name]
            logger.debug('Deleting data for Collection: {} Database: {}'.format(name, database_name))
            result = collection.delete_many(
                {
                    'generation_date': generation_date.strftime('%Y-%m-%d'),
                    'source': source
                }
            )
            logger.debug('Deleted {} Collection {} documents on generated date {} source {}'.format(
                name, result.deleted_count, generation_date, source)
            )

    @classmethod
    def delete_metrics(cls, metric_names, generation_date, source):
        cls.delete_collections(cls.METRIC_DATABASE_NAME, metric_names, generation_date, source)

    @classmethod
    def delete_objects(cls, object_names, generation_date, source):
        cls.delete_collections(cls.OBJECT_DATABASE_NAME, object_names, generation_date, source)

    @classmethod
    def delete_relations(cls, relations_names, generation_date, source):
        cls.delete_collections(cls.RELATIONS_DATABASE_NAME, relations_names, generation_date, source)

    @classmethod
    def merge_relations(cls, relation_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            if 'relation_id' in copied_data:
                del copied_data['relation_id']
            return copied_data

        # TODO change logic
        db = cls.connection(cls.RELATIONS_DATABASE_NAME)
        for rel in relation_collection.items():
            collection = db[rel.name]
            rel_ids = [rel_record[rel.id_field_name] for rel_record in rel.records]
            cursor = collection.aggregate([
                {'$match': {'date': rel.string_date, rel.id_field_name: {'$nin': rel_ids}}},
                {'$group': {'_id': '$' + rel.id_field_name, 'data': {'$last': '$$ROOT'}}}
            ])
            index = 0
            for index, res in enumerate(cursor, start=1):
                rel.add_record(convert(res['data']))

            logger.debug(
                'Merged relation {}, {} found records to update from database against {} records from'
                ' source on {}'.format(rel.name, index, len(rel.records), rel.string_date))

    @classmethod
    def get_relations(cls, relation_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            if 'relation_id' in copied_data:
                del copied_data['relation_id']
            return copied_data

        # TODO change logic
        db = cls.connection(cls.RELATIONS_DATABASE_NAME)
        for rel in relation_collection.items():
            collection = db[rel.name]
            logger.debug('Getting data Relation: {} Date: {}'.format(rel.name, rel.string_date))
            cursor = collection.find({'date': rel.string_date})
            index = 0
            # TODO remove this logic.
            rel.clean_records()
            for index, record in enumerate(cursor, start=1):
                rel.add_record(convert(record))

            logger.debug(
                'Merged relation {}, {} found records to update from database against {} records from'
                ' source on {}'.format(rel.name, index, len(rel.records), rel.string_date))

    @classmethod
    def merge_objects(cls, object_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            return copied_data

        db = cls.connection(cls.OBJECT_DATABASE_NAME)
        for obj in object_collection.items():
            if not obj.is_valid:
                logger.debug('Skipping merging {} object on {}'.format(obj.name, obj.string_date))
                continue

            collection = db[obj.name]
            object_ids = [obj_record[obj.id_field_name] for obj_record in obj.records_with_original_names]
            cursor = collection.aggregate([
                {'$match': {'date': obj.string_date, obj.id_field_name: {'$nin': object_ids}}},
                {'$group': {'_id': '$' + obj.id_field_name, 'data': {'$last': '$$ROOT'}}}
            ])
            index = 0
            for index, res in enumerate(cursor, start=1):
                obj.add_record(convert(res['data']))

            logger.debug('Merged object {}, {} found records to be updated from database against {} records '
                         'from source on {}'.format(obj.name, index, len(obj.records), obj.string_date))

    @classmethod
    def get_objects(cls, object_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            return copied_data

        db = cls.connection(cls.OBJECT_DATABASE_NAME)
        for obj in object_collection.items():
            if not obj.is_valid:
                logger.debug('Skipping merging {} object on {}'.format(obj.name, obj.string_date))
                continue

            collection = db[obj.name]
            logger.debug('Getting data Objects Collection: {} Date: {}'.format(obj.name, obj.string_date))
            cursor = collection.find({'date': obj.string_date})
            index = 0
            # TODO remove this logic.
            obj.clean_records()
            for index, record in enumerate(cursor, start=1):
                obj.add_record(convert(record))

            logger.debug('Merged object {}, {} found records to be updated from database against {} records '
                         'from source on {}'.format(obj.name, index, len(obj.records), obj.string_date))

    @classmethod
    def merge_metrics(cls, metric_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            return copied_data

        db = cls.connection(cls.METRIC_DATABASE_NAME)
        for met in metric_collection.items():
            collection = db[met.name]
            cursor = collection.find({'date': met.string_date})
            index = 0
            for index, record in enumerate(cursor, start=1):
                met.add_record(convert(record))
            logger.debug('Merged metric {}, {} records from database against {} records from source on {}'.format(
                met.name, index, len(met.records), met.string_date)
            )

    @classmethod
    def get_metrics(cls, metric_collection):
        def convert(data):
            copied_data = copy.copy(data)
            if '_id' in copied_data:
                del copied_data['_id']
            if 'generation_date' in copied_data:
                del copied_data['generation_date']
            if 'date' in copied_data:
                del copied_data['date']
            if 'source' in copied_data:
                del copied_data['source']
            return copied_data

        db = cls.connection(cls.METRIC_DATABASE_NAME)
        for met in metric_collection.items():
            collection = db[met.name]
            logger.debug('Getting data Metric: {} Date: {}'.format(met.name, met.string_date))
            cursor = collection.find({'date': met.string_date})
            index = 0
            met.clean_records()
            for index, record in enumerate(cursor, start=1):
                met.add_record(convert(record))
            logger.debug('Merged metric {}, {} records from database against {} records from source on {}'.format(
                met.name, index, len(met.records), met.string_date)
            )

    @classmethod
    def save_metrics(cls, metric_collection, generation_date, source):
        db = cls.connection(cls.METRIC_DATABASE_NAME)
        for met in metric_collection.items():
            collection = db[met.name]
            records = met.records
            logger.debug('Saving {} records for {} metric on {} source {}'.format(len(records), met.name, met.date, source))
            copied_records = []
            for rec in records:
                copied_rec = copy.copy(rec)
                copied_rec.update({
                    'generation_date': generation_date.strftime('%Y-%m-%d'),
                    'date': met.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Metric: {}'.format(met.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} metric {} records on {}'.format(
                met.name, len(result.inserted_ids), met.string_date))

    @classmethod
    def save_objects(cls, object_collection, generation_date, source):
        db = cls.connection(cls.OBJECT_DATABASE_NAME)
        for obj in object_collection.items():
            if not obj.is_valid:
                logger.debug('Skipping saving Object {} for invalid object on {} source {}'.format(
                    obj.name, generation_date, source)
                )
                continue
            collection = db[obj.name]
            records = obj.records_with_original_names
            logger.debug('Saving {} records for {} object on {} source {}'.format(
                len(records), obj.name, obj.date, source))
            copied_records = []
            for record in records:
                copied_rec = copy.copy(record)
                copied_rec.update({
                    'generation_date': generation_date.strftime('%Y-%m-%d'),
                    'date': obj.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Object: {}'.format(obj.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} object {} records on {}'.format(
                obj.name, len(result.inserted_ids), obj.string_date))

    @classmethod
    def save_relations(cls, relation_collection, generation_date, source):
        db = cls.connection(cls.RELATIONS_DATABASE_NAME)
        for rel in relation_collection.items():
            if not rel.is_valid:
                logger.debug('Skipping saving Relation {} for invalid relation on {} source {}'.format(
                    rel.name, generation_date, source)
                )
                continue
            collection = db[rel.name]
            records = rel.records
            logger.debug('Saving {} records for {} relation on {} source {}'.format(
                len(records), rel.name, rel.date, source))
            copied_records = []
            for record in records:
                copied_rec = copy.copy(record)
                copied_rec.update({
                    'generation_date': generation_date.strftime('%Y-%m-%d'),
                    'date': rel.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Relation: {}'.format(rel.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} relation {} records on {}'.format(
                rel.name, len(result.inserted_ids), rel.string_date))
