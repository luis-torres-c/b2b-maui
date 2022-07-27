import requests
import time
import os
import datetime
from utils.logger import logger


def do_request(
        url,
        sessions_requests,
        method,
        payload=None,
        referer=None,
        headers=None,
        tries=0,
        verify_ssl=False,
        allow_redirects=True,
        timeout=500):

    logger.debug('Making Request to {}'.format(url))
    response = None
    if method == 'POST':
        try:
            if not headers:
                if not referer:
                    headers = dict(Referer=url)
                else:
                    headers = dict(Referer=referer)

            response = sessions_requests.post(
                url,
                data=payload,
                headers=headers,
                verify=verify_ssl,
                timeout=timeout,
                allow_redirects=allow_redirects
            )
        except requests.exceptions.Timeout as errt:
            logger.error('Timeout Error: ', errt)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
            else:
                raise B2BTimeoutRequestError
        except requests.exceptions.HTTPError as errh:
            logger.error('Http Error: ', errh)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
        except requests.exceptions.ConnectionError as errc:
            logger.error('ConnectionError ', errc)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
        except requests.exceptions.RequestException as err:
            # Don't try a request again when an exception is unknown
            logger.error('Something Else ', err)

    elif method == 'GET':
        try:
            if not headers:
                response = sessions_requests.get(url, verify=verify_ssl, timeout=timeout, allow_redirects=allow_redirects)
            else:
                response = sessions_requests.get(url, headers=headers, verify=verify_ssl, timeout=timeout, allow_redirects=allow_redirects)
        except requests.exceptions.Timeout as errt:
            logger.error('Timeout Error: ', errt)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
        except requests.exceptions.HTTPError as errh:
            logger.error('Http Error: ', errh)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
        except requests.exceptions.ConnectionError as errc:
            logger.error('ConnectionError ', errc)
            if tries < 5:
                tries += 1
                logger.debug('Retrying requests in 10 seconds')
                time.sleep(10)
                response = do_request(
                    url,
                    sessions_requests,
                    method,
                    payload,
                    referer,
                    headers,
                    tries,
                    verify_ssl,
                    allow_redirects,
                    timeout)
        except requests.exceptions.RequestException as err:
            # Don't try a request again when an exception is unknown
            logger.error('Something Else ', err)
    return response


class ConnectorB2BLoginErrorConnection(Exception):

    def __init__(self, args):
        self.username = args.get('username', '')
        self.portal = args.get('portal', '')

        logger.error("Bad credencial for portal {} whit the user {}".format(self.portal, self.username))

    def __str__(self):
        return repr(self.username)


class ConnectorB2BRedirectionError(Exception):
    pass


class CaptchaErrorException(Exception):
    pass


def SaveErrorLogWhenBadCredentials(log, int_path, portal_name):
    # This function is temporal to analyze login behavior on b2b portals. But it must be removed after that, we
    # can't have a separated log file from docker log handler
    folder = 'log'
    datetime_string = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    output_object_file = '{}/{}/{}_{}.log'.format(int_path, folder, portal_name, datetime_string)
    os.makedirs(os.path.dirname(output_object_file), exist_ok=True)
    logger.debug("Save error page log in {}".format(output_object_file))
    with open(output_object_file, 'w') as lf:
        lf.write(str(log))


class WalmartReportWaitingTimeExceded(Exception):
    pass


class BBrEcommerceErrorClientInPortal(Exception):
    pass


class ConnectorB2BClientInPortalError(Exception):
    pass


class NotCredentialsProvidedError(Exception):
    def __init__(self):
        logger.error("Not Credentials Provided")


class ConnectorB2BGenericError(Exception):
    pass


class B2BTimeoutRequestError(Exception):
    pass


class ConnectorB2BInformationNotAvailable(Exception):
    pass


class CredentialWithInsufficientAccessError(Exception):
    pass
