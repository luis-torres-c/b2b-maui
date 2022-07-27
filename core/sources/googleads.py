import datetime
import re

from core.sources.base import Source
from core.connectors.googleads import GoogleAdsConnector
from core.utils import datetime_to_wivo_format_with_tz, datetime_utc_to_timezone

from utils.logger import logger


class GoogleAdsSource(Source):
    CONNECTOR = GoogleAdsConnector

    CAMPAIGN_SERVICE_NAME = 'CampaignService'
    AD_GROUP_SERVICE_NAME = 'AdGroupService'
    AD_GROUP_AD_SERVICE_NAME = 'AdGroupAdService'

    LABEL_NAME_PATTERN = re.compile('^(product_id:[0-9]+)$')

    CAMPAIGN_STATUS_VALUES = {'REMOVED': 0, 'ENABLED': 1, 'PAUSED': 2}
    AD_GROUP_STATUS_VALUES = {'REMOVED': 0, 'ENABLED': 1, 'PAUSED': 2}
    AD_STATUS_VALUES = {'DISABLED': 0, 'ENABLED': 1, 'PAUSED': 2}

    @classmethod
    def settings(cls):
        import os
        client_id = os.environ['ADWORDS_CLIENT_ID']
        client_secret = os.environ['ADWORDS_CLIENT_SECRET']
        refresh_token = os.environ['ADWORDS_REFRESH_TOKEN']
        developer_token = os.environ['ADWORDS_DEVELOPER_TOKEN']
        client_customer_id = os.environ['ADWORDS_CUSTOMER_CLIENT_ID']
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']

        return {
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'developer_token': developer_token,
            'client_customer_id': client_customer_id,
            'storage_path': storage_path,
            'timezone_name': timezone_name,
        }

    def process(self, **kwargs):
        logger.info('Process google ads...')
        timezone_name = kwargs['timezone_name']
        conn = self.CONNECTOR.get_instance(**kwargs)
        all_data = []

        date_time = datetime.datetime.now()
        string_datetime = datetime_to_wivo_format_with_tz(date_time, timezone_name)
        dt_tz = datetime_utc_to_timezone(date_time, timezone_name)

        for campaign in self.campaigns(conn):
            campaign_id = campaign['id']
            campaign_name = campaign['name']

            logger.info('Processing campaign {}'.format(campaign_name))

            for group in self.ad_groups_by_campaign(conn, campaign_id):
                group_id = group['id']
                group_name = group['name']

                logger.info('Processing ad groups {}'.format(group_name))

                for ad in self.ads_by_group(conn, group_id):
                    logger.info('Processing ad ID {}'.format(ad['ad']['id']))

                    if 'labels' in ad:
                        for label in ad['labels']:
                            name = label['name']

                            if self.LABEL_NAME_PATTERN.match(name):

                                logger.info('Processing Label {}'.format(name))
                                value = self.calculate_value(campaign['status'], group['status'], ad['status'])

                                product_id = re.split(':', name)[1]
                                ad_id = str(ad['ad']['id'])

                                ad_name = '{}_{}_{}'.format(campaign_name, group_name, ad_id)

                                data = {
                                    'datetime': string_datetime,
                                    'value': value,
                                    'product_id': str(product_id),
                                    'googleadwordscampaign_id': str(campaign_id),
                                    'googleadwordscampaign_name': campaign_name,
                                    'googleadwordsgroup_id': str(group_id),
                                    'googleadwordsgroup_name': group_name,
                                    'googleadwordsad_id': ad_id,
                                    'googleadwordsad_name': ad_name
                                }

                                all_data.append(data)

                            else:
                                logger.warning('Label "{}" with wrong format - ignored'.format(name))
                    else:
                        logger.warning('Ad ID {} without label'.format(ad['ad']['id']))

        return [{'metric': 'googleadwordsadstate', 'date': dt_tz.date(), 'records': all_data}]

    def calculate_value(self, campaign_status, group_status, ad_status):
        value = self.AD_STATUS_VALUES.get(ad_status, -1)
        if value is 1:
            group_value = self.AD_GROUP_STATUS_VALUES.get(group_status, -1)
            if group_value is 1:
                value = self.CAMPAIGN_STATUS_VALUES.get(campaign_status, -1)
            else:
                value = group_value

        return value

    def campaigns(self, connector):
        campaign_service = connector.service(self.CAMPAIGN_SERVICE_NAME)
        fields = ['Id', 'Name', 'Status']

        return self.get_pages(campaign_service, fields, [])

    def ad_groups_by_campaign(self, connector, campaing):
        ad_groups_service = connector.service(self.AD_GROUP_SERVICE_NAME)
        fields = ['Id', 'Name', 'Status']
        predicates = [
            {
                'field': 'CampaignId',
                'operator': 'EQUALS',
                'values': campaing
            }
        ]

        return self.get_pages(ad_groups_service, fields, predicates)

    def ads_by_group(self, connector, ad_group):
        ads_service = connector.service(self.AD_GROUP_AD_SERVICE_NAME)
        fields = ['Id', 'Name', 'Status', 'Labels']
        predicates = [
            {
                'field': 'AdGroupId',
                'operator': 'EQUALS',
                'values': ad_group
            }
        ]

        return self.get_pages(ads_service, fields, predicates)

    def get_pages(self, ads_service, fields, predicates):
        pages = []

        offset = 0
        page_size = 500

        selector = {
            'fields': fields,
            'predicates': predicates,
            'paging': {
                'startIndex': str(offset),
                'numberResults': str(page_size)
            }
        }

        more_pages = True
        while more_pages:
            page = ads_service.get(selector)

            pages.extend(self.get_entries(page))

            offset += page_size
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])

        return pages

    @staticmethod
    def get_entries(page):
        if 'entries' in page:
            return page['entries']
        else:
            return []
