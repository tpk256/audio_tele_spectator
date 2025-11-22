import asyncio
import datetime
import os

from pyrogram import Client as ClientTelegram
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler
from pyrogram.filters import group
from gspread import Client as ClientTable, Spreadsheet, Worksheet, service_account
from dotenv import load_dotenv

load_dotenv()

class State:
    WAITING = 'Подвешенный'
    WORKING = 'В работе'
    SUCCESS = 'Решен'



table_url_source = os.getenv('table_url_source')

session_name = os.getenv('session_name')
worksheet_title_source = os.getenv('worksheet_title_source')



def client_init_json() -> ClientTable:
    """Создание клиента для работы с Google Sheets."""
    return service_account(filename='audio-tele-a3bde8a2a231.json')


def get_table_by_url(client: ClientTable, table_url: str):
    """Получение таблицы из Google Sheets по ссылке."""
    return client.open_by_url(table_url)





async def main(telegram_session: str, table_url_source: str, worksheet_title_source: str):
    client_table: ClientTable = client_init_json()
    table_source = get_table_by_url(client_table, table_url_source)
    worksheet_source = table_source.worksheet(worksheet_title_source)


    table_url_destination = os.getenv('table_url_destination')
    worksheet_title_destination = os.getenv('worksheet_title_destination')

    table_destination = get_table_by_url(client_table, table_url_destination)
    worksheet_destination = table_destination.worksheet(worksheet_title_destination)

    client_telegram: ClientTelegram = ClientTelegram(telegram_session)
    await client_telegram.start()

    while True:
        headers = worksheet_source.row_values(1)
        records = worksheet_source.get_all_values()[1:]

        headers += [str(datetime.datetime.now())]
        rows_to_dest = []
        for row_id, record in enumerate(records, 2):
            state, reff = record[2 : 4]

            if state == State.WAITING:
                if reff.startswith('https://t.me/c/'):
                    answers = []
                    str_chat_id, msg_id = reff.replace('https://t.me/c/', '').split('/')

                    chat_id = int("-100" + str_chat_id)
                    msg_id = int(msg_id)
                    try:
                        if await client_telegram.get_discussion_replies_count(chat_id, msg_id) > 0:
                            async for msg in client_telegram.get_discussion_replies(chat_id, msg_id):
                                answers.append(f"https://t.me/c/{str_chat_id}/{msg.id}")

                            print("Row: ", row_id, answers)
                    except FloodWait as exp:
                        print(f"Row: {row_id} получили флуд на ", exp.value, " с.")
                        await asyncio.sleep(exp.value + 0.5)
                        if await client_telegram.get_discussion_replies_count(chat_id, msg_id) > 0:
                            async for msg in client_telegram.get_discussion_replies(chat_id, msg_id):
                                answers.append(f"https://t.me/c/{str_chat_id}/{msg.id}")

                            print("Row: ", row_id, answers)
                    except Exception as exp:
                        print(f"Row: {row_id} is error")
                        print(exp)
                    await asyncio.sleep(1.5)
                    rows_to_dest += [
                        record + [str(answers)]
                    ]


        worksheet_destination.clear()
        rows_to_dest.insert(0, headers)
        worksheet_destination.insert_rows(rows_to_dest, row=1)
        await asyncio.sleep(60)




asyncio.run(main(session_name, table_url_source, worksheet_title_source))
