import os
import requests
import time

from core.connectors.b2b.utils import do_request
from utils.logger import logger


def resolve_recaptcha(google_key, url_home):
    CAPTCHA_GET = "http://2captcha.com/in.php?key={}&method=userrecaptcha&googlekey={}&pageurl={}"
    CAPTCHA_ANSWER = "http://2captcha.com/res.php?key={}&action=get&id={}"
    api_key = os.environ.get('KEY_2CAPTCHA', 'fec5e84812ba342d910e518b41bfcd1e')

    logger.debug("Starting to resolve captcha")
    logger.debug(f"Starting to resolve captcha with key {api_key}")
    logger.debug(f"using google captcha key {google_key}")
    logger.debug(f"from url {url_home}")

    session = requests.session()

    response = do_request(CAPTCHA_GET.format(api_key, google_key, url_home), session, 'POST')
    if '|' not in response.text:
        logger.error("Communication Error, trying again")
        return resolve_recaptcha(google_key, url_home)
    id_response = response.text.split('|')[1]

    captcha_ready = False
    attemp = 1

    while not captcha_ready:
        logger.debug("Wait 20 secconds to resolve recaptcha")
        logger.debug(f"Getting response for attemp {attemp}")

        time.sleep(20)

        resolution_response = do_request(CAPTCHA_ANSWER.format(api_key, id_response), session, 'GET')
        if 'CAPCHA_NOT_READY' in resolution_response.text:
            logger.debug('Recaptcha is not ready')
            attemp += 1
        elif 'ERROR_CAPTCHA_UNSOLVABLE' in resolution_response.text:
            logger.debug('Recaptcha has an error, trying another recaptcha')
            return ''
        else:
            logger.debug("Recaptcha is solved")
            captcha_ready = True

    return resolution_response.text.split('|')[1]
