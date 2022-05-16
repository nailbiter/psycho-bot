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
from datetime import datetime, timedelta
import re
from telegram import Bot
import os
import time
import pymongo


def parse_time(time, now=None):
    if now is None:
        now = datetime.now()
    if time.startswith("+"):
        dt = now + \
            timedelta(minutes=_simple_math_eval(
                time[1:], number_utils=(int, int)))
    else:
        time_chunks = [time[i:i+2] for i in range(0, len(time), 2)]
        time_chunks = reversed(time_chunks)
        dt = now
        for tc, flag in zip(time_chunks, "minute hour day month year".split(" ")):
            dt = dt.replace(
                **{flag: (2000 if flag == "year" else 0)+int(tc)})
#          dt += timedelta(hours=_common.get_current_offset() -
#                          self._timezone_shift.get_timezone_shift())

    dt = _align_datetime(dt)
    return dt


def get_current_offset():
    # code below is adapted from https://stackoverflow.com/a/10854983
    offset = time.timezone if (
        time.localtime().tm_isdst == 0) else time.altzone
    offset_hour = int(offset / 60 / 60 * -1)
    return offset_hour


def _align_datetime(dt):
    return dt.replace(second=0, microsecond=0)


def _simple_math_eval(s, number_utils=(float, float)):
    s = list(s[::-1])
    string_to_num, float_to_num = number_utils

    def get_value():
        sign = 1
        if s and s[-1] == "-":
            s.pop()
            sign = -1
        value = 0
        while s and s[-1].isdigit():
            value *= 10
            value += string_to_num(s.pop())
        return sign * value

    def get_term():
        term = get_value()
        while s and s[-1] in "*/":
            op = s.pop()
            value = get_value()
            if op == "*":
                term *= value
            else:
                term = float_to_num(1.0 * term / value)
        return term

    ans = get_term()
    while s:
        op, term = s.pop(), get_term()
        if op == "+":
            ans += term
        else:
            ans -= term
    return ans


def send_message(chat_id, text, telegram_token=None, enclose_in_triple_ticks=False, **kwargs):
    if telegram_token is None:
        telegram_token = os.environ["TELEGRAM_TOKEN"]
    bot = Bot(telegram_token)

    for text_ in _split_long_text(text, _TELEGRAM_MESSAGE_LEN_LIM):
        if enclose_in_triple_ticks:
            text_ = f"```{text_}```"
        mess = bot.sendMessage(
            chat_id=chat_id,
            text=text_,
            **kwargs
        )


# https://dev-qa.com/320717/sending-large-messages-telegram-bot
#_TELEGRAM_MESSAGE_LEN_LIM = 4096
_TELEGRAM_MESSAGE_LEN_LIM = 4000


def _split_long_text(text, max_len, line_sep="\n"):
    res = []
    accum, buf = 0, []
    for line in text.split(line_sep):
        assert len(line) <= max_len, f"len(\"{line}\")={len(line)}>{max_len}"
        if accum+len(line) > max_len:
            res.append(line_sep.join(buf))
            accum, buf = len(line), [line]
        else:
            accum += (len(line)+len(line_sep))
            buf.append(line)
    if len(buf) > 0:
        res.append(line_sep.join(buf))
    return res
