"""===============================================================================

        FILE: telegram_system/server.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-04-02T22:56:29.570874
    REVISION: ---

==============================================================================="""
from flask import Flask, request
import logging
import os
import requests
import json
import re
from datetime import datetime, timedelta
import _common
import pandas as pd
import io

app = Flask(__name__)


@app.route('/psycho', methods=["POST"])
def psycho():
    #    logging.warning((request.form, os.environ.get("MONGO_URL")))
    logging.warning("psycho!")
    return 'Hello, World!'


@app.route('/message', methods=["POST"])
def message():
    logging.warning((request.form, os.environ.get("MONGO_URL")))
    return 'Hello, World!'
