import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os
class BitrixAPI:
    def __init__(self, webhook_url: str, tz_name: str):
        self.webhook_url = webhook_url
        self.tz_name = tz_name

    def get_weekly_tasks(self):
        """
        Возвращает задачи Bitrix24, созданные за текущую неделю (с понедельника).
        :param webhook_url: полный URL входящего вебхука (https://portal.bitrix24.ru/rest/XX/xxxxxx/)
        :param user_id: ID пользователя (RESPONSIBLE_ID), None — все задачи
        :param tz_name: название часового пояса для расчёта дат
        :return: словарь (JSON) с задачами
        """
        tz = ZoneInfo(self.tz_name)
        now = datetime.now(tz)
        monday = now - timedelta(days=now.weekday())
        monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59)

        # фильтр: задачи, созданные в текущую неделю
        filter_ = {
            f">=CREATED_DATE": monday.isoformat(),
            f"<=CREATED_DATE": sunday.isoformat()
        }

        payload = {
            "filter": filter_,
            "select": ["ID", "TITLE", "RESPONSIBLE_ID", "CREATED_DATE", "DEADLINE", "STATUS"],
            "order": {"CREATED_DATE": "asc"},
            "start": 0
        }

        url = self.webhook_url.rstrip("/") + "/tasks.task.list.json"
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()
