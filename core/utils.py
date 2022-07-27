import calendar
from datetime import datetime
import json
import re
import time

import requests
import string
import zlib
import urllib3
import os

from collections import defaultdict
from utils.logger import logger

import pytz

NON_ALPHANUMERIC_PATTERN = re.compile('[\W_]+')
DATETIME_WIVO_FORMAT = '%Y-%m-%dT%H:%M:%S'
urllib3.disable_warnings()


def apply_prefix(prefix, value):
    return str(prefix) + str(value)


def strips_non_alphanumeric(value):
    return NON_ALPHANUMERIC_PATTERN.sub('', value)


def remove_whitespaces(string_value):
    return string_value.replace(' ', '')


def create_id(string_value):
    """
    Create an ascii alphanumeric ID from string.
    Note: Whitespaces are discarded
    :param string_value:
    :return:
    """
    f = filter(lambda x: x in set(string.printable), string_value)
    new_value = ''.join(list(f))
    new_value = new_value.lower().replace(' ', '')
    # TODO verify hardcoded encoding here
    return strips_non_alphanumeric(new_value) or str(
        (zlib.crc32(string_value.encode('utf-8')) & 0xffffffff))


def default_empty_value():
    return 'EMPTYVALUE'


def string_to_float(s):
    return float(s.replace(',', '.'))


def datetime_utc_to_timezone(date_time, timezone_name):
    tz = pytz.timezone(timezone_name)
    dt_tz = pytz.utc.localize(date_time, is_dst=None).astimezone(tz)
    return dt_tz


def datetime_change_timezone(date_time_with_timezone: [str, datetime], timezone_name: str):
    if isinstance(date_time_with_timezone, str):
        date_time_with_timezone = datetime.fromisoformat(date_time_with_timezone)
    tz = pytz.timezone(timezone_name)
    dt_tz = date_time_with_timezone.astimezone(tz)
    return dt_tz


def datetime_tz_to_utc(date_time):
    return date_time.astimezone(pytz.utc)


def datetime_to_wivo_format_with_tz(date_time, timezone_name):
    dt_tz = datetime_utc_to_timezone(date_time, timezone_name)
    return dt_tz.strftime(DATETIME_WIVO_FORMAT)


def timestamp_to_wivo_format(timestamp):
    return datetime.utcfromtimestamp(
        timestamp).strftime(DATETIME_WIVO_FORMAT)


def datetime_to_wivo_format(date_time):
    return date_time.strftime(DATETIME_WIVO_FORMAT)


def datetime_to_timestamp(dt):
    return calendar.timegm(dt.timetuple())


def makes_request(url, headers=None, method='GET', to_json=True, timeout=180):
    logger.debug(f'Make Request to {url} {method} TimeOut {timeout}')
    try:
        if headers:
            response = requests.request(method, url, headers=headers, timeout=timeout)
        else:
            response = requests.request(method, url, timeout=timeout)
    except requests.exceptions.RequestException:
        logger.warning("There was an error trying to make a first request, trying again...")
        time.sleep(5)
        return makes_request(url, headers, method, to_json, timeout)
    if to_json:
        try:
            return response.json()
        except ValueError:
            logger.warning("Error getting json data, trying again..")
            return makes_request(url, headers=headers, method=method, to_json=to_json, timeout=timeout)
    else:
        return response


class ObjectCollection:

    def __init__(self):
        self.collections = defaultdict(lambda: defaultdict(list))
        self.unique_ids = defaultdict(lambda: defaultdict(set))

    def add_entry(
            self,
            object_name,
            object_id,
            object_value,
            date=None,
            **metadata):
        if object_name in self.unique_ids[date] and object_id in self.unique_ids[date][object_name]:
            # the object is already in the collection
            return

        entry = {
            '{}_id'.format(object_name): object_id,
            '{}_name'.format(object_name): object_value,
        }

        entry.update({'{}_{}'.format(object_name, k): v for k, v in metadata.items()})
        self.unique_ids[date][object_name].add(object_id)
        self.collections[date][object_name].append(entry)

    def get_objects(self):
        return [{'object': k, 'date': d, 'records': v}
                for d, col in self.collections.items() for k, v in col.items()]


def save_state(tag, client=None, status=None):
    from conf.settings import PROD_ENV
    # Function save_state must be executed on production only.
    if not PROD_ENV:
        return
    logger.info("Saving state")
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Token ac7d85d1cb6f18072667f5224397b22874c06d58",
    }
    url_base = 'https://shishio.wivoanalytics.com/v1/accounts/'

    if not client:
        client = os.environ.get('DASHBOARD_NAME', None)
        if not client:
            return

    url = '{url_b}?client={client}&tag={tag}&format=json'.format(url_b=url_base, client=client, tag=tag)

    http = urllib3.PoolManager()
    try:
        r = http.request('GET', url, headers=headers)

        data = json.loads(r.data)

    except Exception as e:
        logger.error("Error while obtained configurations")
        logger.error("Error {}".format(e))
        return

    if len(data) == 0:
        return
    data = data[0]
    id_data = data.pop('id')

    url = '{}{}/'.format(url_base, id_data)

    new_params = list()
    for param in data['params']:
        if param['key'] not in ['latest_status_update', 'status']:
            new_params.append(param)
    new_params.append({
        'key': 'latest_status_update',
        'value': datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    })

    if status:
        new_params.append({
            'key': 'status',
            'value': status
        })
    data['params'] = new_params

    try:
        http.request('PUT', url, headers=headers, body=json.dumps(data))
    except Exception as e:
        logger.error("Error while obtained configurations")
        logger.error("Error {}".format(e))
