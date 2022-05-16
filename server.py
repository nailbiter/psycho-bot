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


@app.route('/new_habit', methods=["POST"])
def new_habit():
    #    logging.warning((request.form, os.environ["MONGO_URL"]))
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]
    text = message["text"]
    _, c1, c2, c3, c4, c5, media, msg = re.split(r"\s+", text, maxsplit=7)
    url = f"http://{os.environ['SCHEDULER']}/register_regular_call"
    _url = f"http://{os.environ['SELF_URL']}/send_message"
    payload = {
        "text": msg,
        "chat_id": chat_id,
    }
    kwargs = {
        "cronline": " ".join([c1, c2, c3, c4, c5]),
        "start_date": datetime.now().isoformat(),
    }
    requests.post(url, data={
        "url": _url,
        "method": "POST",
        "payload": json.dumps(payload),
        **kwargs,
    })
    return 'Hello, World!'


@app.route('/list_timers', methods=["POST"])
def list_timers():
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]
    r = requests.get(f"http://{os.environ['SCHEDULER']}/list_timers")
    df = pd.read_json(io.StringIO(r.text))
    _common.send_message(
        chat_id, df.to_string(), enclose_in_triple_ticks=True, parse_mode="Markdown")
    return 'Success'


@app.route('/list_habits', methods=["POST"])
def list_habits():
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]
    r = requests.get(f"http://{os.environ['SCHEDULER']}/list_habits")
    df = pd.read_json(io.StringIO(r.text))
    _common.send_message(
        chat_id, f"```{df.to_string()}```", parse_mode="Markdown")
    return 'Success'


@app.route('/new_timer', methods=["POST"])
def new_timer():
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]
    try:
        text = message["text"]
        logging.warning(f"\"{text}\"")
        spl = re.split(r"\s+", text, maxsplit=3)
        logging.warning(spl)
        _, time, media, msg = spl
        url = f"http://{os.environ['SCHEDULER']}/register_single_call"
        _url = f"http://{os.environ['SELF_URL']}/send_message"
        payload = {"text": msg, "chat_id": chat_id}
        dt = _common.parse_time(time)
        assert dt > datetime.now(), f"{dt}<={datetime.now()}"
        requests.post(url, data={"datetime": dt.isoformat(
        ), "url": _url, "method": "POST", "payload": json.dumps(payload)})
        _common.send_message(
            chat_id, f"set message `{msg}` to be sent at `{dt.strftime('%Y-%m-%d %H:%M (%a)')}`", parse_mode="Markdown")
    except Exception as e:
        _common.send_message(
            chat_id, f"exception: ``` {e}```", parse_mode="Markdown")
        raise
    return 'Hello, World!'


@app.route("/send_message", methods=["POST"])
def send_message():
    form = request.form
    _common.send_message(form["chat_id"], form["text"])
    return "Success"
