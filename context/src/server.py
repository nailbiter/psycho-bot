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
import requests
import json
import re
from datetime import datetime, timedelta
import pandas as pd
import os
from _common import get_psycho_transitions, get_mongo_client, get_random_uuid, get_condition, get_config
import _common.base

app = Flask(__name__)


@app.route('/activity_sleep', methods=["POST"])
def activity_sleep():
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]
    coll, config = get_config()

    is_sleeping = config.get("is_sleeping", False)
    _common.base.send_message(
        chat_id, f"is_sleeping: `{is_sleeping}` => `{not is_sleeping}`")
    config["is_sleeping"] = not is_sleeping

    coll.delete_many({})
    coll.insert_many([{"key": k, "value": v}for k, v in config.items()])
    return "Hello, World!"


@app.route('/activity_reminder', methods=["POST"])
def activity_reminder():
    message = (request.form)
    _, config = get_config()
    is_sleeping = config.get("is_sleeping", False)
    chat_id = int(os.environ["CHAT_ID"])

    dt = datetime.now()
    _SEC_IN_HOUR = 60*60
    dt = datetime.fromtimestamp(
        round(dt.timestamp()/_SEC_IN_HOUR)*_SEC_IN_HOUR)
    dt -= timedelta(hours=1)

    if is_sleeping:
        _common.base.send_message(
            chat_id, f"sleeping ==> no message ({df.strftime('%H:%M')})")
    else:
        df = get_psycho_transitions("data/activities.json")
        _, coll = get_mongo_client("activities")

        _uuid = get_random_uuid()

        message_id = _common.base.send_message(
            chat_id,
            render_template(
                "psycho.jinja.md",
                uuid=_uuid,
                df=df,
                text=df.text.iloc[0],
                dt=dt,
            )
        )

        coll.insert_one({
            "datetime": _common.base.to_utc_datetime(dt),
            "uuid": _uuid,
            "message_id": message_id,
            "_real_datetime": _common.base.to_utc_datetime(),
            "chat_id": chat_id,
        })

    return "Hello, World!"


@app.route('/ls', methods=["POST"])
def ls():
    message = json.loads(request.form["message"])
    df = get_psycho_transitions("./data/psycho.json")
    _, coll = get_mongo_client("psycho1")
    coll_df = pd.DataFrame(coll.find())
    coll_df = pd.DataFrame([
        {
            "uuid": uuid_,
            "cnt": len(slice_),
        }
        for uuid_, slice_
        in coll_df.groupby("uuid")
    ])
    coll_df = coll_df[coll_df.cnt < (len(df)+1)]
    coll_df.uuid = "#"+coll_df.uuid
    coll_df = coll_df.set_index("uuid").sort_values(by="cnt", ascending=False)
    coll_df.cnt = coll_df.cnt.apply(lambda cnt: f"{cnt}/{len(df)+1}")
    text = f"""have {len(coll_df)} incomplete:
    {coll_df.to_string()}
    """
    _common.base.send_message(
        message["chat"]["id"], text)

    return "Hello, World"


@app.route('/psycho', methods=["POST"])
def psycho():
    message = json.loads(request.form["message"])
    df = get_psycho_transitions("data/psycho.json")
    _, coll = get_mongo_client("psycho1")

    text = message["text"].strip()
    now = datetime.now()
    if text == "/psycho":
        dt = now
    else:
        _, dt = re.split(r"\s+", text, maxsplit=1)
        dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")

    _uuid = get_random_uuid()
    chat_id = message["chat"]["id"]
    message_id = _common.base.send_message(
        chat_id,
        render_template(
            "psycho.jinja.md",
            uuid=_uuid,
            df=df,
            text=df.text.iloc[0],
        )
    )

    coll.insert_one({
        "datetime": _common.base.to_utc_datetime(dt),
        "uuid": _uuid,
        "message_id": message_id,
        "_real_datetime": _common.base.to_utc_datetime(now),
        "chat_id": chat_id,
    })
    return 'Hello, World!'


@app.route('/message', methods=["POST"])
def message():
    message = json.loads(request.form["message"])
    chat_id = message["chat"]["id"]

    is_caught = False
    for file_name, coll_name in zip(["psycho", "activities"], ["psycho1", "activities"]):
        df = get_psycho_transitions(f"data/{file_name}.json")
        _, coll = get_mongo_client(coll_name)

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
            if len(coll_df) != 1:
                continue
            else:
                logging.warning(f"caught by {file_name}!")
                is_caught = True

            cnt = coll_df.cnt.iloc[0]

            cond = df.condition.iloc[cnt-1]
            if not pd.isna(cond):
                assert get_condition(cond)(message["text"])

            message_id = None
            _uuid = coll_df.uuid.iloc[0]
            # FIXME: push this logic to template
            if cnt < len(df):
                text = df.text.iloc[cnt]
                message_id = _common.base.send_message(
                    chat_id,
                    render_template(
                        "psycho.jinja.md",
                        cnt=cnt,
                        uuid=_uuid,
                        df=df,
                        text=text,
                    ),
                )
            else:
                _common.base.send_message(chat_id, f"finish #{_uuid}")

            coll.insert_one({
                "datetime": _common.base.to_utc_datetime(),
                "uuid": _uuid,
                "message_id": message_id,
                "text": message["text"],
            })
        except Exception as e:
            _common.base.send_message(
                chat_id, f"exception: ``` {e}```", parse_mode="Markdown")
            raise e
        if is_caught:
            break

    if not is_caught:
        _common.base.send_message(chat_id, "not caught")
    return 'Hello, World!'
