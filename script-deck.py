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
import logging
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import _common
import numpy as np
from os import path


@click.group()
def script_deck():
    pass


@script_deck.command()
@click.option("--mark-printed/--no-mark-printed", " /-n", default=True)
@click.option("-k", "--key", type=click.Choice(list(_common.load_data_json("collections")[1]))
def print_psycho(mark_printed, key):
    _, _D=_common.load_data_json("collections")
    file_name, coll_name=_D[key]
    _, coll=_common.get_mongo_client(coll_name)
    coll_df=pd.DataFrame(coll.find())
    _, coll_print_marks=_common.get_mongo_client("psycho1_print_marks")
    states_df=pd.read_json(_common.load_data_json(file_name)[0])
    print_marks_df=pd.DataFrame(coll_print_marks.find())
    print_marks_df=print_marks_df[["uuid"]]
    print_marks_df["is_printed"]=True
    coll_df=pd.concat([
        pd.DataFrame({
            **slice_.sort_values(by="datetime")[["text", "uuid"]][1:],
            "i":np.arange(len(slice_)-1),
            "datetime":_common.to_utc_datetime(slice_.datetime.min(), inverse=True),
        })
        for uuid_, slice_
        in coll_df.groupby("uuid")
        if len(slice_) == (len(states_df)+1)
    ])
    coll_df=coll_df.set_index("uuid").join(
        print_marks_df.set_index("uuid")).reset_index()
    coll_df.is_printed=coll_df.is_printed.fillna(False)
    coll_df=coll_df[~coll_df.is_printed]
    assert len(coll_df) > 0
    uuids=coll_df.pop("uuid").unique()
    coll_df=coll_df.set_index(["datetime", "i"]).unstack().text
    coll_df=coll_df.reset_index()
    logging.warning((list(coll_df), len(coll_df)))
    coll_df=coll_df.sort_values(by="datetime")

    # postproc
    if key == "activities":
        dt=coll_df.pop("datetime")
        coll_df.insert(loc=0, column="day", value=dt.apply(
            lambda dt: dt.strftime("%Y-%m-%d")))
        coll_df.insert(loc=1, column="h1", value=dt.apply(
            lambda dt: dt.strftime("%H:00")))
        coll_df.insert(loc=2, column="h2", value=(
            dt+timedelta(hours=1)).apply(lambda dt: dt.strftime("%H:00")))

        trip=coll_df.pop(2).str.split(
            "/").apply(lambda l: map(int, l)).apply(list)
#        click.echo(pd.DataFrame(list(trip)))
        coll_df[[f"s{i}"for i in range(3)]]=pd.DataFrame(list(trip))
    else:
        pass

    click.echo(coll_df.to_csv(header=None, index=None, sep="\t"))
    if mark_printed:
        dt=datetime.now()
        coll_print_marks.insert_many([{"uuid": u, "dt": dt} for u in uuids])
        logging.warning(f"{len(coll_df)} marks done")


@ script_deck.command()
@ click.option("--habits-file", type=click.Path(), default="habits.json")
@ click.option("--db-name", default="psycho_timers")
@ click.option("--coll-name", default="habits")
@ click.option("--mongo-url", envvar="MONGO_URL", required=True)
def load_habits(habits_file, mongo_url, coll_name, db_name):
    habits_df=pd.read_json(habits_file)
    habits_df["start_date"]=datetime.now()-timedelta(hours=9) - \
        timedelta(minutes=2)
    click.echo(habits_df.to_string(index=None))
    coll=pymongo.MongoClient(mongo_url)[db_name][coll_name]
    coll.delete_many({})
    coll.insert_many(habits_df.to_dict(orient="records"))


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    script_deck()
