from dotenv import load_dotenv
import os
import sys

load_dotenv(".env")

if 'unittest' in sys.modules.keys():
    load_dotenv(".env.test", override=True)


class Settings:
    host = os.getenv("HOST")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("USER")
    openai_api_key = os.getenv("CHATGPT_API_KEY")
