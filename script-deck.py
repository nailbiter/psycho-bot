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

@click.group()
def script_deck():
    pass

@script_deck.command()
def print():
    pass

@script_deck.command()
@click.option("--habits-file",type=click.Path(),default="habits.json")
def load_habits(habits_file):
    habits_df = pd.read_json(habits_file)
    click.echo(habits_df.to_string(index=None))

if __name__=="__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    script_deck()
