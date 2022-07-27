from core.fetcher.base import DailyFetcher
from core.sources.googleads import GoogleAdsSource
from core.storages.base import GenericMetricsObjectsCSVStorage


class GoogleAdsFetcher(GenericMetricsObjectsCSVStorage, GoogleAdsSource, DailyFetcher):
    name = 'google-ads'
    # This Fetcher doesnt need a consolidation process
    PROCESS_BY_RANGE_DAYS_ENABLE = False
