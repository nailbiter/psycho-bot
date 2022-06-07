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
import itertools
import fractions
from dotenv import load_dotenv
import logging
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import _common
import _common.google_drive
import numpy as np
from os import path
import tqdm
import string


@click.group()
def script_deck():
    pass


@script_deck.command()
@click.option("--mark-printed/--no-mark-printed", " /-n", default=True)
@click.option("-k", "--key", type=click.Choice(list(_common.load_data_json("collections")[1])))
def print_psycho(mark_printed, key):
    _, _D = _common.load_data_json("collections")
    file_name, coll_name = _D[key]
    _, coll = _common.get_mongo_client(coll_name)

    coll_df = pd.DataFrame(coll.find())
    _, coll_print_marks = _common.get_mongo_client("psycho1_print_marks")
    states_df = pd.read_json(_common.load_data_json(file_name)[0])
    print_marks_df = pd.DataFrame(coll_print_marks.find())
    print_marks_df = print_marks_df[["uuid"]]
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
    assert len(coll_df) > 0
    uuids = coll_df.pop("uuid").unique()
    coll_df = coll_df.set_index(["datetime", "i"]).unstack().text
    coll_df = coll_df.reset_index()
    logging.warning((list(coll_df), len(coll_df)))
    coll_df = coll_df.sort_values(by="datetime")

    # postproc
    if key == "activities":
        dt = coll_df.pop("datetime")
        coll_df.insert(loc=0, column="day", value=dt.apply(
            lambda dt: dt.strftime("%Y-%m-%d")))
        coll_df.insert(loc=1, column="h1", value=dt.apply(
            lambda dt: dt.strftime("%H:00")))
        coll_df.insert(loc=2, column="h2", value=(
            dt+timedelta(hours=1)).apply(lambda dt: dt.strftime("%H:00")))

        trip = coll_df.pop(2).str.split(
            "/").apply(lambda l: map(int, l)).apply(list)
#        click.echo(pd.DataFrame(list(trip)))
        coll_df[[f"s{i}"for i in range(3)]] = pd.DataFrame(list(trip))
    else:
        pass

    text = coll_df.to_csv(header=None, index=None, sep="\t")
    _fn = _common.get_random_filename(".csv")
    with open(_fn, "w") as f:
        f.write(text)
    logging.warning(f"output duplicated to {_fn}")

    click.echo(text)
    if mark_printed:
        dt = datetime.now()
        coll_print_marks.insert_many([{"uuid": u, "dt": dt} for u in uuids])
        logging.warning(f"{len(coll_df)} marks done")
    else:
        logging.warning(f"no marks added")


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


@script_deck.command()
@click.option("-k", "--key", type=click.Choice(list(_common.load_data_json("collections")[1])), required=True)
@click.option("--dry-run/--no-dry-run", default=True)
@click.argument("src")
@click.argument("dst", nargs=-1)
def cp(key, dry_run, src, dst):
    _, _D = _common.load_data_json("collections")
    file_name, coll_name = _D[key]
    _, coll = _common.get_mongo_client(coll_name)
    coll_df = pd.DataFrame(coll.find())
    states_df = pd.read_json(_common.load_data_json(file_name)[0])

    src, dst = src[1:], [tag[1:] for tag in dst]
    click.echo((src, dst))

    src_df = coll_df[coll_df.uuid == src]
    src_df = src_df.sort_values(by="datetime", ignore_index=True)
    assert len(src_df) == len(states_df)+1, src_df
    click.echo(src_df)

    dst_df = coll_df[coll_df.uuid.apply(lambda uuid_:uuid_ in dst)]
    click.echo(dst_df)
    assert len(dst_df) == len(dst)

    for (i, r), (dst_uuid, dst_dt) in tqdm.tqdm(list(itertools.product(list(enumerate(src_df.to_dict(orient="records")))[1:], dst_df[["uuid", "datetime"]].values))):
        r = {k: v for k, v in r.items() if not k.startswith("_")}
        new_r = {**r, "datetime": dst_dt +
                 timedelta(minutes=1), "uuid": dst_uuid}
        for k, v in new_r.items():
            if pd.isna(v):
                new_r[k] = None
        click.echo(new_r)
        if not dry_run:
            coll.insert_one(new_r)

    if dry_run:
        click.echo(" dry run")
    else:
        click.echo("no dry run")


@script_deck.command()
@click.option("-k", "--key", type=click.Choice(list(_common.load_data_json("collections")[1])))
@click.option("-h", "--head", type=int)
@click.option("-d", "--delete", type=(int, int))
@click.option("--dry-run/--no-dry-run", default=True)
@click.option("-u", "--upper", default=1, type=float)
@click.option("-f", "--fill", type=(int, int, str, str, str))
def show_incomplete(key, head, dry_run, delete, fill, upper):
    _, _D = _common.load_data_json("collections")
    file_name, coll_name = _D[key]
    _, coll = _common.get_mongo_client(coll_name)
    coll_df = pd.DataFrame(coll.find())
    states_df = pd.read_json(_common.load_data_json(file_name)[0])

    coll_df = pd.DataFrame([
        {
            "uuid": uuid_,
            "datetime": _common.to_utc_datetime(slice_.datetime.min(), inverse=True),
            "phase": fractions.Fraction(len(slice_)-1, len(states_df)),
            "text": {text: dt for text, dt in slice_[["text", "datetime"]].values if not pd.isna(text)},
        }
        for uuid_, slice_
        in coll_df.groupby("uuid")
        if len(slice_) < (len(states_df)+1)
    ])
    if len(coll_df) == 0:
        click.echo(f"no \"{key}\" key!")
        exit(0)
    coll_df["text"] = coll_df.pop("text").apply(
        lambda d: None if len(d) == 0 else min(d, key=d.get))
    coll_df = coll_df.sort_values(by="datetime", ignore_index=True)
    coll_df = coll_df[coll_df.phase <= upper]

    click.echo(f"{len(coll_df)} pending")
    if head is not None:
        coll_df = coll_df.head(head)
    click.echo(coll_df)

    if delete is not None:
        to_delete_df = coll_df.iloc[delete[0]:delete[1]+1]
        click.echo(f"the following will be deleted!:\n{to_delete_df}")
        if not dry_run:
            click.echo("no dry run")
            for uuid_ in tqdm.tqdm(to_delete_df.uuid):
                coll.delete_one({"uuid": uuid_})
        else:
            click.echo("dry run")
    elif fill is not None:
        to_insert_df = coll_df.iloc[fill[0]:fill[1]+1]
        if not dry_run:
            click.echo("no dry run")
            now_ = datetime.now()
            for (uuid_, dt), (i, text) in tqdm.tqdm(list(itertools.product(zip(to_insert_df.uuid, to_insert_df.datetime), enumerate(fill[2:])))):
                coll.insert_one(
                    {"uuid": uuid_, "datetime": dt+timedelta(minutes=i+10), "text": text})
        else:
            click.echo("dry run")


@script_deck.command()
@click.option("--client-key", type=click.Path(), default="client_secret.json")
@click.option("--token-key", type=click.Path(), default=".token.json")
@click.option("--spreadsheet-id", envvar="PSYCHO_TABLE", required=True)
def show_incomplete_spreadsheet(client_key, token_key, spreadsheet_id):
    creds = _common.google_drive.get_creds(
        client_key, token_key, create_if_not_exist=True)
    df = _common.google_drive.download_df_from_google_sheets(
        creds, spreadsheet_id, initial_row=1)
    _, trans = _common.load_data_json("psycho")

    df = pd.DataFrame({(trans[i-1]["text"] if i > 0 else "dt"): col for i,
                      (cn, col) in enumerate(df.items())})
    df["i"] = df.index+3

    missing_df = pd.DataFrame([
        {**r,
            "empty_f": next(filter(lambda t: pd.isna(t[1][1]), enumerate(r.items())))}
        for r
        in df.to_dict(orient="records")
        if sum(map(pd.isna, r.values())) > 0
    ])
    missing_df.empty_f = missing_df.empty_f.apply(
        lambda t: (string.ascii_uppercase[t[0]], t[1][0]))
    missing_df = missing_df.set_index("i")
    click.echo(missing_df)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    script_deck()
