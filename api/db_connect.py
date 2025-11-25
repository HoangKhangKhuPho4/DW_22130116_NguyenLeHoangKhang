import pymysql
from util.config import get_db_config

def dw():
    cfg = get_db_config()
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["db_name"],
        cursorclass=pymysql.cursors.DictCursor
    )

def mart():
    cfg = get_db_config()
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["mart_schema"],
        cursorclass=pymysql.cursors.DictCursor
    )
