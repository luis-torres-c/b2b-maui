import csv
import copy
import os
import re

import inflection

from core.storages.base import Storage, MetricCollection, ObjectCollection, RelationCollection

from utils.wmongo import MongoDbManager
from utils.logger import logger

class ValidationDataError(Exception):
    pass


class ReplacingLogicCSVStorage(Storage):

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

        def trim(data):
            for d in data:
                for rec in d['records']:
                    for k, v in rec.items():
                        if isinstance(v, str):
                            rec[k] = v.strip()

        storage_path = kwargs['storage_path']

        default_date = self.actual_date_tz or self.actual_date

        # Updating dates for those metrics that don't have a defined date.
        # using actual date.
        update_dates(metrics, default_date)

        # Trim all string from records
        trim(metrics)

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
                    HistoricalData.delete_metrics(metric_collection, self.name)
                HistoricalData.delete_objects(object_collection, self.name)
                # copied collections from original generation to be saved in database.
                new_metric_collection = MetricCollection(copy.deepcopy(metrics))
                new_object_collection = metric_collection.get_object_collection()
                HistoricalData.save_objects(new_object_collection, self.name)
                if self.HISTORICAL_METRIC_ENABLE:
                    HistoricalData.save_metrics(new_metric_collection, self.name)

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
                    HistoricalData.delete_metrics(metric_collection, self.name)
                HistoricalData.delete_objects(object_collection, self.name)
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
                    HistoricalData.save_metrics(new_metric_collection, self.name)
                HistoricalData.save_objects(new_object_collection, self.name)

    def _consolidate_objects(self, objects, **kwargs):

        if not objects:
            return

        def update_dates(data, date):
            for d in data:
                if not ('date' in d and d.get('date')):
                    d['date'] = date

        def trim(data):
            for d in data:
                for rec in d['records']:
                    for k, v in rec.items():
                        if isinstance(v, str):
                            rec[k] = v.strip()

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

        default_date = self.actual_date_tz or self.actual_date

        update_dates(objects, default_date)

        trim(objects)

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
                HistoricalData.delete_objects(object_collection, self.name)
                # copied collections from original generation to be saved in database.
                new_object_collection = ObjectCollection(copy.deepcopy(new_objects))
                HistoricalData.save_objects(new_object_collection, self.name)

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
                HistoricalData.delete_objects(object_collection, self.name)
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
                HistoricalData.save_objects(new_object_collection, self.name)

    def _consolidate_relations(self, relations, **kwargs):

        if not relations:
            return

        def update_dates(data, date):
            for d in data:
                if not ('date' in d and d.get('date')):
                    d['date'] = date

        def trim(data):
            for d in data:
                for rec in d['records']:
                    for k, v in rec.items():
                        if isinstance(v, str):
                            rec[k] = v.strip()

        default_date = self.actual_date_tz or self.actual_date

        update_dates(relations, default_date)

        trim(relations)

        relation_collection = RelationCollection(relations)

        if self.STORAGE_LOGIC_WITH_INDEXES:
            if self.HISTORICAL_CACHE_ENABLE:
                # Creating Indexes if doesnt exist
                HistoricalData.create_indexes_for_relations(relation_collection)
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_relations(relation_collection, self.name)
                # copied collections from original generation to be saved in database.
                new_relation_collection = RelationCollection(copy.deepcopy(relations))
                HistoricalData.save_relations(new_relation_collection, self.name)

                # Adding complementary data from database.
                HistoricalData.get_relations(relation_collection)

            storage_path = kwargs['storage_path']
            for relation in relation_collection.items():
                self._create_relation(storage_path, relation)
        else:
            if self.HISTORICAL_CACHE_ENABLE:
                # Deleting previous data for this range time (generation date)
                HistoricalData.delete_relations(relation_collection, self.name)
                # copied collections from original generation to be saved in database.
                new_relation_collection = RelationCollection(copy.deepcopy(relations))
                # Adding complementary data from database.
                HistoricalData.merge_relations(relation_collection)

            storage_path = kwargs['storage_path']
            for relation in relation_collection.items():
                self._create_relation(storage_path, relation)

            if self.HISTORICAL_CACHE_ENABLE:
                HistoricalData.save_relations(new_relation_collection, self.name)

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

        self.validate_metrics(metrics_data)
        self.validate_objects(objects_data)
        self.validate_relations(relations_data)
        self._consolidate_metrics(metrics_data, **kwargs)
        self._consolidate_objects(objects_data, **kwargs)
        self._consolidate_relations(relations_data, **kwargs)

    @classmethod
    def validate_metrics(cls, metrics_data):
        for d in metrics_data:
            for rec in d['records']:
                try:
                    float(rec['value'])
                except (ValueError, TypeError) as e:
                    raise ValidationDataError(
                        'Metric name {} has a record\'s value that is not numeric details: {} {}'.format(
                            d['metric'], rec, e))

    @classmethod
    def validate_objects(cls, object_data):
        pass

    @classmethod
    def validate_relations(cls, relations_data):
        pass


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
            if not any([True for i in index_names if re.search(r'date_\d_source_\d', i)]):
                logger.debug('Creating Index GENERATION_DATA/SOURCE for {} collection {} database'.format(
                    col_name, database_name))
                collection.create_index([
                    ('date', cls.ASCENDING),
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
    def delete_collections(cls, database_name, name_vs_string_dates_pairs, source):
        db = cls.connection(database_name)
        for name, string_date in name_vs_string_dates_pairs:
            collection = db[name]
            logger.debug('Deleting data for Collection: {} Database: {}'.format(name, database_name))
            result = collection.delete_many(
                {
                    'date': string_date,
                    'source': source
                }
            )
            logger.debug('Deleted {} Collection {} documents on generated date {} source {}'.format(
                name, result.deleted_count, string_date, source)
            )

    @classmethod
    def delete_metrics(cls, metric_collection, source):
        pairs = [(metric.name, metric.string_date) for metric in metric_collection.items()]
        cls.delete_collections(cls.METRIC_DATABASE_NAME, pairs, source)

    @classmethod
    def delete_objects(cls, object_collection, source):
        pairs = [(obj.name, obj.string_date) for obj in object_collection.items()]
        cls.delete_collections(cls.OBJECT_DATABASE_NAME, pairs, source)

    @classmethod
    def delete_relations(cls, relation_collection, source):
        pairs = [(obj.name, obj.string_date) for obj in relation_collection.items()]
        cls.delete_collections(cls.RELATIONS_DATABASE_NAME, pairs, source)

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
    def save_metrics(cls, metric_collection, source):
        db = cls.connection(cls.METRIC_DATABASE_NAME)
        for met in metric_collection.items():
            collection = db[met.name]
            records = met.records
            logger.debug('Saving {} records for {} metric on {} source {}'.format(
                len(records), met.name, met.date, source))
            copied_records = []
            for rec in records:
                copied_rec = copy.copy(rec)
                copied_rec.update({
                    'date': met.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Metric: {}'.format(met.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} metric {} records on {}'.format(
                met.name, len(result.inserted_ids), met.string_date))

    @classmethod
    def save_objects(cls, object_collection, source):
        db = cls.connection(cls.OBJECT_DATABASE_NAME)
        for obj in object_collection.items():
            if not obj.is_valid:
                logger.debug('Skipping saving Object {} for invalid object on {} source {}'.format(
                    obj.name, obj.string_date, source)
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
                    'date': obj.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Object: {}'.format(obj.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} object {} records on {}'.format(
                obj.name, len(result.inserted_ids), obj.string_date))

    @classmethod
    def save_relations(cls, relation_collection, source):
        db = cls.connection(cls.RELATIONS_DATABASE_NAME)
        for rel in relation_collection.items():
            if not rel.is_valid:
                logger.debug('Skipping saving Relation {} for invalid relation on {} source {}'.format(
                    rel.name, rel.string_date, source)
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
                    'date': rel.string_date,
                    'source': source,
                })
                copied_records.append(copied_rec)

            logger.debug('Inserting data Relation: {}'.format(rel.name))
            result = collection.insert_many(copied_records)
            logger.debug('Inserted new {} relation {} records on {}'.format(
                rel.name, len(result.inserted_ids), rel.string_date))
