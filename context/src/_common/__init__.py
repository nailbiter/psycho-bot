"""===============================================================================

        FILE: _common.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2022-04-03T22:08:04.028205
    REVISION: ---

==============================================================================="""
import logging
import os
import requests
import json
import re
from datetime import datetime, timedelta
import pandas as pd
import io
#import uuid
import pymongo
from jinja2 import Template
import randomname


def get_psycho_transitions(fn):
    df = pd.read_json(fn)
    return df


def get_mongo_client(collname):
    client = pymongo.MongoClient(os.environ["MONGO_URL"])
    coll = client.psychobot[collname]
    return client, coll


def get_random_uuid():
    #    return str(uuid.uuid4())
    return randomname.generate().replace("-", "_")


class _IntCondition():
    def __init__(self, lower_bound=None, upper_bound=None):
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def __call__(self, text):
        i = int(text)
#        logging.error(
#            f"test {i} with {(self._lower_bound, self._upper_bound)}")
        if self._lower_bound is not None:
            if i < self._lower_bound:
                return False
        if self._upper_bound is not None:
            if i > self._upper_bound:
                return False
        return True


_CONDITIONS = {
    "int": _IntCondition,
}


def get_condition(cond):
    type_ = cond.pop("type")
    return _CONDITIONS[type_](**cond)


def get_config():
    _, coll = get_mongo_client("config")
    config = {
        r["key"]: r["value"]
        for r
        in coll.find()
    }
    return coll, config
