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
from flask import Flask, request, render_template
import logging
import os
import requests
import json
import re
from datetime import datetime, timedelta
import _common
import pandas as pd
import io
import uuid
import pymongo
from jinja2 import Template

app = Flask(__name__)


def _get_psycho_transitions(fn):
    df = pd.read_json(fn)
    return df


def _get_mongo_client(collname):
    client = pymongo.MongoClient(os.environ["MONGO_URL"])
    coll = client.psychobot[collname]
    return client, coll


@app.route('/ls', methods=["POST"])
def ls():
    message = json.loads(request.form["message"])
    df = _get_psycho_transitions("data/psycho.json")
    _, coll = _get_mongo_client("psycho1")
    coll_df = pd.DataFrame(coll.find())
    coll_df = pd.DataFrame([
        {
            "uuid": uuid_,
            "cnt": len(slice_),
            #            "message_id": slice_.message_id.iloc[slice_.datetime.argmax()],
            #            "text":message["text"],
        }
        for uuid_, slice_
        in coll_df.groupby("uuid")
    ])
    coll_df = coll_df[coll_df.cnt < (len(df)+1)]
    coll_df = coll_df.set_index("uuid").sort_values(by="cnt", ascending=False)
    coll_df.cnt = coll_df.cnt.apply(lambda cnt: f"{cnt}/{len(df)+1}")
    text = f"""have {len(coll_df)} incomplete: ```
    {coll_df.to_string()}
    ```"""
    _common.send_message(message["chat"]["id"], text, parse_mode="Markdown")

    return "Hello, World"


@app.route('/psycho', methods=["POST"])
def psycho():
    message = json.loads(request.form["message"])
    df = _get_psycho_transitions("data/psycho.json")
    _, coll = _get_mongo_client("psycho1")

    text = message["text"].strip()
    now = datetime.now()
    if text == "/psycho":
        dt = now
    else:
        _, dt = re.split(r"\s+", text, maxsplit=1)
        dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")

    _uuid = str(uuid.uuid4())
    chat_id = message["chat"]["id"]
    message_id = _common.send_message(
        chat_id,
        render_template(
            "psycho.jinja.md",
            uuid=_uuid,
            df=df,
            text=df.text.iloc[0],
        )
    )

    coll.insert_one({
        "datetime": _common.to_utc_datetime(dt),
        "uuid": _uuid,
        "message_id": message_id,
        "_real_datetime": _common.to_utc_datetime(now),
        "chat_id": chat_id,
    })
    return 'Hello, World!'


@app.route('/message', methods=["POST"])
def message():
    message = json.loads(request.form["message"])
    df = _get_psycho_transitions("data/psycho.json")
    _, coll = _get_mongo_client("psycho1")

    try:
        coll_df = pd.DataFrame(coll.find())
        coll_df = pd.DataFrame([
            {
                "uuid": uuid_,
                "cnt": len(slice_),
                "message_id": slice_.message_id.iloc[slice_.datetime.argmax()],
                "text":message["text"],
            }
            for uuid_, slice_
            in coll_df.groupby("uuid")
        ])
        coll_df = coll_df[coll_df.message_id ==
                          message["reply_to_message"]["message_id"]]
        assert len(coll_df) == 1, coll_df

        cnt = coll_df.cnt.iloc[0]

        cond = df.condition.iloc[cnt-1]
        if cond == "0-100":
            assert 0 <= int(message["text"]) <= 100
        else:
            pass

        message_id = None
        if cnt < len(df):
            text = df.text.iloc[cnt]
            message_id = _common.send_message(
                message["chat"]["id"],
                render_template(
                    "psycho.jinja.md",
                    cnt=cnt,
                    uuid=coll_df.uuid.iloc[0],
                    df=df,
                    text=text,
                ),
            )
        else:
            _common.send_message(message["chat"]["id"], "finish")

        coll.insert_one({
            "datetime": _common.to_utc_datetime(),
            "uuid": coll_df.uuid.iloc[0],
            "message_id": message_id,
            "text": message["text"],
        })
    except Exception as e:
        _common.send_message(
            message["chat"]["id"], f"exception: ``` {e}```", parse_mode="Markdown")
        raise e

    return 'Hello, World!'
