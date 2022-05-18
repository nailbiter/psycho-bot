#!/usr/bin/env python3
"""===============================================================================

        FILE: script-deck.py

       USAGE: ./script-deck.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-05-16T22:16:13.911043
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import _common
import numpy as np


@click.group()
def script_deck():
    pass


@script_deck.command()
@click.option("--mark-printed/--no-mark-printed", default=True)
def print_psycho(mark_printed):
    _, coll = _common.get_mongo_client("psycho1")
    coll_df = pd.DataFrame(coll.find())
    _, coll_print_marks = _common.get_mongo_client("psycho1_print_marks")
    states_df = pd.read_json("context/src/data/psycho.json")
#    click.echo(len(states_df))
    print_marks_df = pd.DataFrame(coll_print_marks.find())
    print_marks_df["is_printed"] = True
    coll_df = pd.concat([
        pd.DataFrame({
            **slice_.sort_values(by="datetime")[["text", "uuid"]][1:],
            "i":np.arange(len(slice_)-1),
            "datetime":_common.to_utc_datetime(slice_.datetime.min(), inverse=True),
        })
        for uuid_, slice_
        in coll_df.groupby("uuid")
        if len(slice_) == (len(states_df)+1)
    ])
    coll_df = coll_df.set_index("uuid").join(
        print_marks_df.set_index("uuid")).reset_index()
    coll_df.is_printed = coll_df.is_printed.fillna(False)
    coll_df = coll_df[~coll_df.is_printed]
    uuids = coll_df.pop("uuid").unique()
    coll_df = coll_df.set_index(["datetime", "i"]).unstack().text
    coll_df = coll_df.reset_index()
    logging.warning((list(coll_df), len(coll_df)))
    coll_df = coll_df.sort_values(by="datetime")
    click.echo(coll_df.to_csv(header=None, index=None, sep="\t"))
    if mark_printed:
        coll_print_marks.insert_many([{"uuid": u}for u in uuids])


@script_deck.command()
@click.option("--habits-file", type=click.Path(), default="habits.json")
@click.option("--db-name", default="psycho_timers")
@click.option("--coll-name", default="habits")
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
def load_habits(habits_file, mongo_url, coll_name, db_name):
    habits_df = pd.read_json(habits_file)
    habits_df["start_date"] = datetime.now()-timedelta(hours=9) - \
        timedelta(minutes=2)
    click.echo(habits_df.to_string(index=None))
    coll = pymongo.MongoClient(mongo_url)[db_name][coll_name]
    coll.delete_many({})
    coll.insert_many(habits_df.to_dict(orient="records"))


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    script_deck()
