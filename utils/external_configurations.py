import urllib3
import json
from utils.logger import logger
import os

urllib3.disable_warnings()


def get_external_configurations(client, tag):
    results = dict()
    file_Path = os.environ.get("PASSWORDS_FILE", "/home/jorgequetgo/Wivo/passwords.txt")
    config_file = open(file_Path, "r")
    for line in config_file:
        data = line.split(";")
        if tag in data[0]:
            results["tag"] = data[0]
            results["tracker"] = data[0]
            results["b2b_username"] = data[1]
            results["b2b_password"] = data[2]
            results["b2b_empresa"] = data[3]
            break
    return results

