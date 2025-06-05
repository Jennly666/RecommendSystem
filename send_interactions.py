import asyncio
import aio_pika
import json
import logging
import random
import asyncpg
from domain.entities import Interaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_video_ids():
    """Получение списка video_id из базы данных."""
    try:
        pool = await asyncpg.create_pool(
            user="postgres",
            password="qwerty",
            database="recommendations",
            host="127.0.0.1"
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id FROM videos")
            video_ids = [row["id"] for row in rows]
        await pool.close()
        return video_ids
    except Exception as e:
        logger.error(f"Failed to fetch video IDs: {e}")
        raise

async def send_interaction(interaction: Interaction):
    """Отправка взаимодействия в очередь RabbitMQ."""
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/", reconnect_interval=10)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("interactions_queue", durable=True)
            message = aio_pika.Message(
                body=json.dumps({
                    "user_id": interaction.user_id,
                    "video_id": interaction.video_id,
                    "action": interaction.action
                }).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            await channel.default_exchange.publish(message, routing_key="interactions_queue")
            logger.info(f"Sent interaction: {interaction}")
    except Exception as e:
        logger.error(f"Failed to send interaction {interaction}: {e}")
        raise

async def generate_random_interaction(video_ids):
    """Генерация случайного взаимодействия."""
    user_id = f"user_{random.randint(1, 150)}"
    video_id = random.choice(video_ids)
    action = random.choice(["view", "like", "comment", "favorite"])
    return Interaction(user_id=user_id, video_id=video_id, action=action)

async def main():
    """Бесконечный цикл отправки случайных взаимодействий."""
    video_ids = await get_video_ids()
    if not video_ids:
        logger.error("No video IDs found")
        return
    logger.info(f"Valid video IDs: {video_ids}")
    while True:
        try:
            interaction = await generate_random_interaction(video_ids)
            await send_interaction(interaction)
            await asyncio.sleep(random.uniform(0.5, 2))
        except Exception as e:
            logger.error(f"Error processing interaction: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())