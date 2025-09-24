"""
Менеджер конфигурации для микросервисов.

Этот модуль предоставляет класс `ConfigManager`, который решает две задачи:
1.  **Чтение конфигурации**: Загружает конфигурацию из Redis при старте.
2.  **Динамическое обновление**: Подписывается на fanout-обменник в RabbitMQ,
    чтобы получать уведомления об изменении конфигурации и обновлять ее
    "на лету", без необходимости перезапуска сервиса.

Это позволяет централизованно управлять поведением всех сервисов.
"""
import asyncio
import json
import logging
from typing import Any, Optional

import aio_pika
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

CONFIG_CACHE_KEY = "config:cache"
CONFIG_UPDATES_EXCHANGE = "config.updates"


class ConfigManager:
    def __init__(self, redis_url: str, rabbitmq_url: str):
        self._redis_url = redis_url
        self._rabbitmq_url = rabbitmq_url
        self._redis: Optional[Redis] = None
        self._config_cache: dict = {}
        self._update_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._is_initialized = False

    async def initialize(self):
        """
        Инициализирует менеджер: подключается к Redis, загружает начальную
        конфигурацию и запускает фоновую задачу для прослушивания обновлений.
        """
        if self._is_initialized:
            return

        async with self._lock:
            if self._is_initialized:
                return

            try:
                self._redis = Redis.from_url(self._redis_url, decode_responses=True)
                await self._load_config_from_redis()
                asyncio.create_task(self._listen_for_updates())
                self._is_initialized = True
                logger.info("ConfigManager initialized successfully.")
            except Exception as e:
                logger.critical(f"Failed to initialize ConfigManager: {e}", exc_info=True)
                # В реальном приложении здесь может быть более сложная логика
                # обработки сбоя инициализации.
                raise

    async def _load_config_from_redis(self):
        """Загружает полную конфигурацию из кэша Redis."""
        if not self._redis:
            raise RuntimeError("Redis connection not available.")
        
        raw_config = await self._redis.get(CONFIG_CACHE_KEY)
        if raw_config:
            self._config_cache = json.loads(raw_config)
            logger.info(f"Configuration loaded from Redis: {self._config_cache}")
        else:
            logger.warning("Configuration cache in Redis is empty. Using default values.")
            self._config_cache = {}

    async def _on_update_message(self, message: aio_pika.IncomingMessage):
        """Callback-функция для обработки сообщений об обновлении конфига."""
        async with message.process():
            logger.info("Received configuration update notification. Reloading from Redis...")
            try:
                await self._load_config_from_redis()
                self._update_event.set()  # Уведомляем всех, кто ждет
                self._update_event.clear() # Сбрасываем для следующего ожидания
            except Exception as e:
                logger.error(f"Failed to process config update: {e}", exc_info=True)

    async def _listen_for_updates(self):
        """
        Основной цикл прослушивания сообщений об обновлении конфигурации.
        Запускается как фоновая задача.
        """
        while True:
            try:
                connection = await aio_pika.connect_robust(self._rabbitmq_url)
                async with connection:
                    channel = await connection.channel()
                    # Объявляем fanout-обменник
                    exchange = await channel.declare_exchange(
                        CONFIG_UPDATES_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True
                    )
                    # Объявляем эксклюзивную временную очередь
                    queue = await channel.declare_queue(exclusive=True)
                    # Привязываем очередь к обменнику
                    await queue.bind(exchange)
                    
                    logger.info(f"Listening for configuration updates on exchange '{CONFIG_UPDATES_EXCHANGE}'...")
                    await queue.consume(self._on_update_message)
                    
                    # Бесконечно ждем, пока соединение активно
                    await asyncio.Future()

            except (aio_pika.exceptions.AMQPConnectionError, ConnectionError) as e:
                logger.error(f"RabbitMQ connection error in config listener: {e}. Retrying in 10s...")
                await asyncio.sleep(10)
            except Exception as e:
                logger.critical(f"Critical error in config listener: {e}", exc_info=True)
                await asyncio.sleep(10)

    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Возвращает значение конфигурации по ключу из кэша.
        """
        return self._config_cache.get(key, default)

    async def wait_for_update(self) -> None:
        """
        Асинхронно ждет следующего события обновления конфигурации.
        """
        await self._update_event.wait()
