import asyncio
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import aiohttp
import redis.asyncio as redis
from producer import PollingProducer

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# --- Константы для Redis ---
USER_MAP_KEY = "user_map"
USER_COMPLETED_TASKS_KEY = "user_completed_total_task"
USER_LIST_KEY = "user_list"


async def fetch_config(session: aiohttp.ClientSession) -> dict:
    """Запрашивает конфигурацию у config-server."""
    config_server_url = os.getenv("CONFIG_SERVER_URL", "http://config-server:8090")
    try:
        async with session.get(f"{config_server_url}/api/v1/configs/all") as resp:
            resp.raise_for_status()
            config = await resp.json()
            logging.info("Конфигурация успешно получена с config-server.")
            return config
    except Exception as e:
        logging.critical(f"Не удалось получить конфигурацию с config-server: {e}")
        raise


async def ensure_user_map(redis_client: redis.Redis, config: dict) -> dict:
    """
    Проверяет наличие user_map в Redis. Если его нет, запускает
    инициализацию через user_initial_server.
    """
    user_map = await redis_client.hgetall(USER_MAP_KEY)
    if user_map:
        logging.info(f"user_map найден в Redis. Пользователей: {len(user_map)}")
        return user_map

    logging.warning("user_map не найден в Redis. Запускаю инициализацию...")
    user_init_server_url = config.get("USER_INITIAL_SERVER_URL")
    if not user_init_server_url:
        raise ValueError("USER_INITIAL_SERVER_URL не настроен в config-server.")

    try:
        async with aiohttp.ClientSession() as session:
            # Отправляем запрос на запуск инициализации
            async with session.post(f"{user_init_server_url}/init") as resp:
                resp.raise_for_status()
                response_data = await resp.json()
                if response_data.get("status") == "init_started":
                    logging.info("Сервер инициализации пользователей запущен. Ожидание завершения...")
                else:
                    logging.error(f"Неожиданный ответ от сервера инициализации: {response_data}")
                    return {}

            # Ожидаем ответа о завершении
            # В реальном проекте здесь лучше использовать WebSocket или long-polling
            while True:
                await asyncio.sleep(10) # Пауза перед проверкой статуса
                async with session.get(f"{user_init_server_url}/status") as resp:
                    if resp.status == 200:
                        status_data = await resp.json()
                        if status_data.get("status") == "init_done":
                            logging.info("Сервер сообщил о завершении инициализации user_map.")
                            # Повторная попытка получить user_map
                            user_map = await redis_client.hgetall(USER_MAP_KEY)
                            if user_map:
                                logging.info(f"user_map успешно получен из Redis. Пользователей: {len(user_map)}")
                                return user_map
                            else:
                                logging.error("Инициализация завершена, но user_map в Redis пуст.")
                                return {}
                    logging.info("Инициализация user_map еще не завершена, ждем...")

    except Exception as e:
        logging.error(f"Ошибка при инициализации user_map: {e}")
        return {}


async def main_polling_loop(producer: PollingProducer, redis_client: redis.Redis, config: dict):
    """Основной цикл работы продюсера."""
    
    # 1. Получаем user_map
    user_map = await ensure_user_map(redis_client, config)
    if not user_map:
        logging.critical("Не удалось получить user_map. Продюсер не может продолжить работу.")
        return

    responsible_ids = list(user_map.keys())
    
    # Сохраняем список ID пользователей в Redis (Set для уникальности)
    await redis_client.sadd(USER_LIST_KEY, *responsible_ids)
    logging.info(f"Список ID пользователей сохранен в Redis ключ '{USER_LIST_KEY}'.")

    # 2. Определяем временной диапазон
    # Например, за последние 7 дней
    tz = timezone(timedelta(hours=int(config.get("TZ_HOURS", 6)))) # Часовой пояс из конфига
    to_dt = datetime.now(tz)
    from_dt = to_dt - timedelta(days=int(config.get("FETCH_DAYS_RANGE", 7)))
    from_iso = from_dt.isoformat()
    to_iso = to_dt.isoformat()
    
    logging.info(f"Диапазон выборки задач: с {from_iso} по {to_iso}")

    # 3. Автовыбор стратегии
    logging.info("Выполняю замер для выбора стратегии...")
    total_tasks = await producer.get_overall_total_completed(from_iso, to_iso)
    total_users = len(responsible_ids)
    pages = math.ceil(total_tasks / 50)
    
    logging.info(f"Замер: Всего задач = {total_tasks}, Всего пользователей = {total_users}, Страниц задач = {pages}")

    totals = {}
    if total_users < pages:
        logging.info("Выбрана стратегия 'by_total' (по пользователям).")
        totals = await producer.get_totals_by_user_via_total(
            responsible_ids=responsible_ids,
            from_iso=from_iso,
            to_iso=to_iso,
            batch_size=int(config.get("BATCH_SIZE", 50)),
            delay_between_batches_ms=int(config.get("DELAY_MS", 200)),
            retries=int(config.get("RETRIES", 3)),
            base_backoff=float(config.get("BASE_BACKOFF", 0.6)),
        )
    else:
        logging.info("Выбрана стратегия 'by_scanning' (сканирование задач).")
        totals = await producer.get_totals_by_scanning_tasks(
            from_iso=from_iso,
            to_iso=to_iso,
            pages_per_batch=int(config.get("PAGES_PER_BATCH", 10)),
            delay_between_batches_ms=int(config.get("DELAY_MS", 200)),
            retries=int(config.get("RETRIES", 3)),
            base_backoff=float(config.get("BASE_BACKOFF", 0.6)),
        )
    
    logging.info(f"Получены данные о {len(totals)} пользователях.")

    # 4. Сохраняем результаты в Redis
    if totals:
        # Используем hset для сохранения словаря в хэш
        await redis_client.hset(USER_COMPLETED_TASKS_KEY, mapping=totals)
        logging.info(f"Данные о выполненных задачах сохранены в Redis ключ '{USER_COMPLETED_TASKS_KEY}'.")
    else:
        logging.info("Нет данных для сохранения в Redis.")


async def main():
    """Главная функция запуска."""
    async with aiohttp.ClientSession() as session:
        config = await fetch_config(session)

    redis_client = redis.from_url(config["REDIS_URL"], decode_responses=True)
    
    producer = PollingProducer(
        webhook_url=config["BITRIX_WEBHOOK_URL"],
        mode="polling"
    )

    # Запуск основного цикла
    # В реальном приложении здесь может быть `while True` с паузой
    try:
        await main_polling_loop(producer, redis_client, config)
    finally:
        await redis_client.close()
        logging.info("Соединение с Redis закрыто.")


if __name__ == "__main__":
    logging.info("Запуск Polling Producer...")
    asyncio.run(main())
    logging.info("Polling Producer завершил работу.")