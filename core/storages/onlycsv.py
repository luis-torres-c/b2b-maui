import csv
import copy
import glob
import os

import inflection

from core.storages.base import Storage, MetricCollection, ObjectCollection, RelationCollection

from utils.logger import logger


class ValidationDataError(Exception):
    pass


class OnlyCsvStorage(Storage):

    def _create_relation(self, root_path, relation):

        suffix = relation.string_date
        prefix = f'{relation.parent_name}->{relation.child_name}'
        source = self.name

        output_relation_file = f'{root_path}/relations/{source}__{prefix}_{suffix}.csv'
        os.makedirs(os.path.dirname(output_relation_file), exist_ok=True)

        # remove old file format if exists
        # This is for drawback compatibility where source prefix doesn't exist
        old_file_path_v1 = f'{root_path}/relations/{prefix}_{suffix}.csv'
        old_file_path_v2 = f'{root_path}/relations/{prefix}-{suffix}.csv'

        old_files = [old_file_path_v1, old_file_path_v2]
        for old_file in old_files:
            if os.path.isfile(old_file):
                os.remove(old_file)
                logger.debug(f'Removed old file {old_file}')

        logger.debug(f'Saving Relation {output_relation_file}')
        with open(output_relation_file, 'w', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=relation.fieldnames)
            writer.writeheader()
            for record in relation.records_with_original_names:
                writer.writerow(record)

    def _create_object(self, root_path, object):

        # FIXME improve this validation, when an object doesnt have the minimum required fields
        if len(object.fieldnames) < 2:
            logger.debug(f'Skipping Object Creation for {object.name}')
            return

        suffix = object.string_date
        pluralized_object_name = object.pluralized_name
        source = self.name

        output_object_file = \
            f'{root_path}/objects/{pluralized_object_name}/{source}__{pluralized_object_name}_{suffix}.csv'
        os.makedirs(os.path.dirname(output_object_file), exist_ok=True)

        # remove old file format if exists
        # This is for drawback compatibility where source prefix doesn't exist
        old_file_path_v1 = \
            f'{root_path}/objects/{pluralized_object_name}/{pluralized_object_name}_{suffix}.csv'
        old_files = [old_file_path_v1]
        for old_file in old_files:
            if os.path.isfile(old_file):
                os.remove(old_file)
                logger.debug(f'Removed old file {old_file}')

        logger.debug(f'Saving Object {output_object_file}')
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

        for metric in metric_collection.items():
            records = metric.records
            if not metric.records:
                continue

            pluralized_met_name = metric.pluralized_name
            metric_fieldnames = metric.metric_fieldnames
            source = self.name

            output_metric_file = \
                f'{storage_path}/metrics/{pluralized_met_name}/{source}__{pluralized_met_name}_{metric.string_date}.csv'
            os.makedirs(os.path.dirname(output_metric_file), exist_ok=True)

            # remove old file format if exists
            # This is for drawback compatibility where source prefix doesn't exist
            old_file_path_v1 = \
                f'{storage_path}/metrics/{pluralized_met_name}/{pluralized_met_name}_{metric.string_date}.csv'
            old_files = [old_file_path_v1]
            for old_file in old_files:
                if os.path.isfile(old_file):
                    os.remove(old_file)
                    logger.debug(f'Removed old file {old_file}')

            logger.debug(f'Saving Metric {output_metric_file}')
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

        for obj in object_collection.items():
            self._create_object(
                storage_path,
                obj,
            )

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

        storage_path = kwargs['storage_path']
        for relation in relation_collection.items():
            self._create_relation(storage_path, relation)

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