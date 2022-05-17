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


@click.group()
def script_deck():
    pass


@script_deck.command()
def print():
    pass


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
