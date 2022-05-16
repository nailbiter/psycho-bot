#!/usr/bin/env python3
# pylint: disable=C0116,W0613
"""===============================================================================

        FILE: telegram_system/client.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-04-01T14:54:31.707825
    REVISION: ---

==============================================================================="""
# This program is dedicated to the public domain under the CC0 license.
# copied from https://github.com/python-telegram-bot/python-telegram-bot/blob/92cb6f3ae8d5c3e49b9019a9348d4408135ffc95/examples/echobot.py

"""
Simple Bot to reply to Telegram messages.

First, a few handler functions are defined. Then, those functions are passed to
the Dispatcher and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.

Usage:
Basic Echobot example, repeats messages.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""


# Enable logging


import logging
import requests
import os
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
import functools
import json
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.
def start(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    update.message.reply_markdown_v2(
        fr'Hi {user.mention_markdown_v2()}\!',
        reply_markup=ForceReply(selective=True),
    )


def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text('Help!')


def process_command(update, context, command=None):
    assert command is not None
    chat_id = update.message.chat_id
    if update.message.chat_id != int(os.environ["CHAT_ID"]):
        logging.warning(
            f"chat_id={chat_id}!={os.environ['CHAT_ID']} ==> ignore")
        return
    url = f"http://{os.environ['SCHEDULER']}/{command}"
    logging.error(update.message.to_dict())
    update.message.chat_id
    message_str = json.dumps(update.message.to_dict())
    logging.warning(message_str)
    requests.post(url, data={"message": message_str})


def main() -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    updater = Updater(os.environ["TELEGRAM_TOKEN"])

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))

    for k in ["new_timer", "new_habit", "list_timers", "list_habits"]:
        dispatcher.add_handler(CommandHandler(
            k, functools.partial(process_command, command=k)))

    # on non command i.e message - echo the message on Telegram
#    dispatcher.add_handler(MessageHandler(
#        Filters.text & ~Filters.command, echo))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
