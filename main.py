import asyncio
import datetime
import os
import json
import time

from pyrogram import Client as ClientTelegram
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler
from pyrogram.filters import group
import gspread_formatting
from gspread import Client as ClientTable, Spreadsheet, Worksheet, service_account
from gspread.utils import ValueInputOption


from dotenv import load_dotenv

load_dotenv()

class State:
    WAITING = 'Подвешенный'
    WORKING = 'В работе'
    SUCCESS = 'Решен'


with open("data.json", "r") as f:
    chats_info = json.load(f)


table_url_source = os.getenv('table_url_source')

session_name = os.getenv('session_name')
worksheet_title_source = os.getenv('worksheet_title_source')



def client_init_json() -> ClientTable:
    """Создание клиента для работы с Google Sheets."""
    return service_account(filename='audio-tele-a3bde8a2a231.json')


def get_table_by_url(client: ClientTable, table_url: str):
    """Получение таблицы из Google Sheets по ссылке."""
    return client.open_by_url(table_url)


def get_batch_colors(red: list[int], green: list[int], gray: list[int]) -> list[dict]:
    cells: list[dict] = [
        {
            "range": f"A1",
            "format": {
                "backgroundColorStyle": {
                    "rgbColor": {
                        "red": 1.0,
                        "green": 0.65,
                        "blue": 0.0,
                        "alpha": 1
                    },

                },
            },
        }
    ]


    for row_id in red:
        cells.append(
            {
                "range": f"D{row_id}:E{row_id}",
                "format": {
                    "backgroundColorStyle": {
                        "rgbColor": {
                            "red": 1,
                            "green": 0,
                            "blue": 0,
                            "alpha": 1
                        },


                    },
                },
            }
        )

    for row_id in green:
        cells.append(
            {
                "range": f"D{row_id}:E{row_id}",
                "format": {
                    "backgroundColorStyle": {
                        "rgbColor": {
                            "red": 0,
                            "green": 1,
                            "blue": 0,
                            "alpha": 1
                        }
                    },
                },
            }
        )

    for row_id in gray:
        cells.append(
            {
                "range": f"D{row_id}:E{row_id}",
                "format": {
                    "backgroundColorStyle": {
                        "rgbColor": {
                            "red": 0.3,
                            "green": 0.3,
                            "blue": 0.3,
                            "alpha": 1
                        }
                    },
                },
            }
        )
    return cells


async def executor(telegram_session: str, table_url_source: str, worksheet_title_source: str, chats_info: dict):
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
        headers = [headers[0], headers[1], headers[3], "Ответ KFC", "Сообщение ОП"]
        records = worksheet_source.get_all_values()[1:]

        rows_to_dest = []

        gray = []
        red = []
        green = []
        dest_id = 3
        for row_id, record in enumerate(records, 2):
            state, reff = record[2: 4]

            if state == State.WAITING:


                if reff.startswith('https://t.me/c/'):

                    answers = []
                    str_chat_id, msg_id = reff.replace('https://t.me/c/', '').split('/')

                    chat_id = int("-100" + str_chat_id)

                    helpers: list[int] = chats_info.get("-100" + str_chat_id)

                    msg_id = int(msg_id)
                    flag_error = False
                    ans_from_helpers = None
                    ask_from_ops = None

                    try:
                        if await client_telegram.get_discussion_replies_count(chat_id, msg_id) > 0:
                            async for msg in client_telegram.get_discussion_replies(chat_id, msg_id):

                                if msg.from_user.id in helpers and ans_from_helpers is None:
                                    ans_from_helpers = msg
                                elif msg.from_user.id not in helpers and ask_from_ops is None:
                                    ask_from_ops = msg
                                else:
                                    break


                        if ans_from_helpers is None:  # Значит, что ответа не было
                            red.append(dest_id)
                            answers = [ "Ответа нет", "-"]

                        elif ans_from_helpers and ask_from_ops is None:
                            green.append(dest_id)
                            txt = "ПРОВЕРЬ ЧАТ! ОТВЕТ НЕ ТЕКСТ"
                            if ans_from_helpers.text:
                                txt = ans_from_helpers.text.replace('"', "'")

                            answers = [
                                f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ans_from_helpers.id}"; "{txt}")',
                                "-"]
                        else:
                            # todo Хелпер мог отредачить сообщение!
                            if ans_from_helpers.date >= ask_from_ops.date:
                                green.append(dest_id)
                            else:
                                red.append(dest_id)

                            txt_helper = "ПРОВЕРЬ ЧАТ! ОТВЕТ НЕ ТЕКСТ"
                            if ans_from_helpers.text:
                                txt_helper = ans_from_helpers.text.replace('"', "'")

                            txt_op = "ПРОВЕРЬ ЧАТ! ВОПРОС ОПЕРАТОРА НЕ ТЕКСТ"
                            if ask_from_ops.text:
                                txt_op = ask_from_ops.text.replace('"', "'")

                            answers = [
                                f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ans_from_helpers.id}"; "{txt_helper}")',
                                f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ask_from_ops.id}"; "{txt_op}")'
                            ]




                    except FloodWait as exp:
                        print(f"Row: {row_id} получили флуд на ", exp.value, " с.")
                        await asyncio.sleep(exp.value + 0.5)
                        if await client_telegram.get_discussion_replies_count(chat_id, msg_id) > 0:
                            async for msg in client_telegram.get_discussion_replies(chat_id, msg_id):

                                if msg.from_user.id in helpers and ans_from_helpers is None:
                                    ans_from_helpers = msg
                                elif msg.from_user.id not in helpers and ask_from_ops is None:
                                    ask_from_ops = msg

                        if ans_from_helpers is None:  # Значит, что ответа не было
                            red.append(dest_id)
                            answers += ["Ответа нет", "-"]

                        elif ans_from_helpers and ask_from_ops is None:
                            green.append(dest_id)
                            txt = "ПРОВЕРЬ ЧАТ! ОТВЕТ НЕ ТЕКСТ"
                            if ans_from_helpers.text:
                                txt = ans_from_helpers.text.replace('"', "'")


                            answers = [
                                f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ans_from_helpers.id}"; "{txt}")',
                                "-"]
                        else:
                            # todo Хелпер мог отредачить сообщение!
                            if ans_from_helpers.date >= ask_from_ops.date:
                                green.append(dest_id)
                            else:
                                red.append(dest_id)

                            txt_helper = "ПРОВЕРЬ ЧАТ! ОТВЕТ НЕ ТЕКСТ"
                            if ans_from_helpers.text:
                                txt_helper = ans_from_helpers.text.replace('"', "'")

                            txt_op = "ПРОВЕРЬ ЧАТ! ВОПРОС ОПЕРАТОРА НЕ ТЕКСТ"
                            if ask_from_ops.text:
                                txt_op = ask_from_ops.text.replace('"', "'")

                            answers = [f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ans_from_helpers.id}"; "{txt_helper}")',
                                        f'=HYPERLINK("https://t.me/c/{str_chat_id}/{ask_from_ops.id}"; "{txt_op}")']


                    except Exception as exp:
                        flag_error = True
                        print(f"Row: {row_id} is error")
                        print(exp)
                    await asyncio.sleep(1.5)
                    if flag_error:
                        rows_to_dest += [["'" + record[0], record[1], record[3], "ОШИБКА: Нужно проверить чат", "ОШИБКА: Нужно проверить чат"]]
                    else:
                        rows_to_dest += [["'" + record[0], record[1], record[3]] + answers]
                    dest_id += 1



        worksheet_destination.clear()

        worksheet_destination.format(f"A1:E{worksheet_destination.row_count}", {
                    "backgroundColorStyle": {
                        "rgbColor": {
                            "red": 1,
                            "green": 1,
                            "blue": 1,
                            "alpha": 1
                        }
                    },
                },)



        rows_to_dest.insert(0, headers)

        tm = time.time()
        dt_not = datetime.datetime.fromtimestamp( tm // 10 * 10)

        rows_to_dest.insert(0, ["'" + str(dt_not)])

        worksheet_destination.insert_rows(rows_to_dest, row=1, value_input_option=ValueInputOption.user_entered)

        worksheet_destination.batch_format(get_batch_colors(red, green, gray))
        await asyncio.sleep(60)


async def main(telegram_session: str, table_url_source: str, worksheet_title_source: str, chats_info: dict):

    # TODO Сделать нормальную обработку ошибок и уведомление об онных сделать
    while True:
        try:
            await executor(telegram_session, table_url_source, worksheet_title_source, chats_info)
        except Exception as exp:
            print(exp, "ТАБЛИЦА НЕ ОБНОВЛЕНА!")






asyncio.run(main(session_name, table_url_source, worksheet_title_source, chats_info))
