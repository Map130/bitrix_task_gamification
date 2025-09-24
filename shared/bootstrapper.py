"""
Этот модуль отвечает за загрузку конфигурации для сервиса при его старте.

Логика работы:
1.  При старте сервис знает только URL `config-server`.
2.  Он делает HTTP-запрос к `config-server` для получения всех необходимых
    параметров конфигурации (URL-ы, токены и т.д.).
3.  Запрос повторяется с экспоненциальной задержкой в случае неудачи,
    чтобы дождаться, пока `config-server` станет доступен.
4.  Полученные конфигурации возвращаются в виде словаря.
"""
import asyncio
import logging
import os
from typing import Dict, Any

import aiohttp

logger = logging.getLogger(__name__)

async def fetch_config_from_server() -> Dict[str, Any]:
    """
    Запрашивает конфигурацию у `config-server`.
    В случае неудачи повторяет запросы с увеличивающейся задержкой.
    """
    config_server_url = os.getenv("CONFIG_SERVER_URL")
    if not config_server_url:
        raise RuntimeError("CONFIG_SERVER_URL is not set. Cannot fetch configuration.")

    url = f"{config_server_url}/api/v1/configs/all"
    max_retries = 5
    backoff_factor = 2
    initial_delay = 1.0

    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    configs = await response.json()
                    logger.info(f"Successfully fetched {len(configs)} config items from config-server.")
                    return configs
        except aiohttp.ClientError as e:
            delay = initial_delay * (backoff_factor ** attempt)
            logger.warning(
                f"Could not connect to config-server at {url} (attempt {attempt + 1}/{max_retries}): {e}. "
                f"Retrying in {delay:.2f} seconds..."
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
            else:
                logger.critical("Failed to fetch configuration from config-server after all retries.")
                raise

    # Этот код не должен быть достижим, но для полноты картины
    raise RuntimeError("Failed to fetch configuration from config-server.")
