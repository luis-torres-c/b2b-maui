import json
import os

import inflection

from utils.logger import logger


def dictionary_with_default_value(data):
    def wrapper(key):
        if isinstance(data, dict):
            return data.get(key)
        elif isinstance(data, str):
            return data
    return wrapper


class TimezonesAndSchedulesMixIn:

    FILE_NAME = 'timezones_schedules.json'

    def post_process(self, processed_data, **kwargs):
        # FIXME SimpleCsvStorage handles another format besides metrics, objects, relation.
        # We are forcing to use this class, although we shouldn't on that case.

        store_object_name = os.environ.get('STORE_OBJECT_NAME', 'bstore')
        default_timezone = os.environ.get('DEFAULT_OBJECT_STORE_TIMEZONE', '')
        base_store_name = inflection.singularize(store_object_name)
        store_names = [
            base_store_name,
            inflection.pluralize(store_object_name),
        ]

        conf_path = os.environ['SOURCE_INT_PATH']
        file_path = os.path.join(conf_path, 'int-config', self.FILE_NAME)
        if os.path.isfile(file_path):
            with open(file_path, 'r') as json_file:
                data_json = json.load(json_file)
                stores_data = dictionary_with_default_value({x['store']: x for x in data_json})
        else:
            logger.debug('Timezone/schedules configuration - {} doesn\'t exist'.format(file_path))
            return processed_data

        for item in processed_data:
            store_id_key = '{}_id'.format(base_store_name)
            store_name_key = '{}_name'.format(base_store_name)

            if 'relation' in item:
                continue

            if not (('metric' in item and item['records'] and store_id_key in item['records'][0] and store_name_key in item['records'][0]) or ('object' in item and item['object'] in store_names)):
                continue

            logger.info('Processing Post-Process Item {}'.format(item.get('metric', item.get('object'))))
            timezone_key_name = '{}_timezone'.format(base_store_name)
            schedule_key_name = '{}_schedule'.format(base_store_name)
            for record in item['records']:
                if store_id_key in record and store_name_key in record:
                    if stores_data(str(record.get(store_id_key))):
                        store_id = record[store_id_key]
                        additional_data = stores_data(str(store_id))
                        timezone_value = additional_data['timezone']
                        record.update({timezone_key_name: timezone_value})
                        schedule_value = json.dumps(additional_data['schedules'])
                        record.update({schedule_key_name: schedule_value})
                    else:
                        record.update({timezone_key_name: default_timezone})
                        record.update({schedule_key_name: {}})

        return processed_data
