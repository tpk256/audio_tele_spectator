import asyncio
import os

from pyrogram import Client

from dotenv import load_dotenv

load_dotenv()


api_id =  int(os.getenv('api_id'))
api_hash = os.getenv('api_hash')


async def main():
    async with Client("audio_tele_spectator", api_id, api_hash) as app:
        await app.send_message("me", "Greetings from **Pyrogram**!")


asyncio.run(main())