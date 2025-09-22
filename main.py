import json
import logging
from core import BitrixSimple
from dotenv import load_dotenv
import os

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_data_as_dict():
    """
    Инициализирует клиент Bitrix, получает еженедельные задачи для активных
    пользователей и возвращает их в виде словаря.
    """
    logging.info("Загрузка переменных окружения...")
    load_dotenv()
    webhook_url = os.getenv("WEBHOOK_URL")

    if not webhook_url:
        logging.error("Переменная окружения 'WEBHOOK_URL' не найдена. Завершение работы.")
        return None

    # Не выводим полный URL в логи из соображений безопасности
    logging.info(f"Инициализация клиента для домена: {webhook_url.split('/rest/')[0]}")

    # Создаем экземпляр клиента
    client = BitrixSimple(webhook_url=webhook_url)

    # 1. Получаем список активных пользователей
    logging.info("Запрос списка активных пользователей...")
    active_users = client.get_active_users()
    logging.info(f"Найдено {len(active_users)} активных пользователей.")

    if not active_users:
        logging.warning("Активные пользователи не найдены. Дальнейшее получение задач бессмысленно.")
        return {}

    # 2. Получаем их задачи за неделю
    logging.info("Запрос еженедельных задач для пользователей...")
    tasks_data = client.get_weekly_tasks_batch(active_users)
    logging.info("Обработка задач завершена.")

    # 3. Возвращаем результат (это уже словарь)
    return tasks_data

# Пример использования функции
if __name__ == "__main__":
    logging.info("--- Запуск скрипта ---")
    users_data = get_data_as_dict()

    if users_data is not None:
        logging.info("Данные успешно получены. Вывод результата.")
        print("\nСловарь с данными о задачах:")
        print(json.dumps(users_data, indent=2, ensure_ascii=False))
    else:
        logging.error("Не удалось получить данные.")
    
    logging.info("--- Скрипт завершил работу ---")
