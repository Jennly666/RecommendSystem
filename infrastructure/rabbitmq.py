import aio_pika
import json
import logging
from domain.entities import Interaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def setup_rabbitmq():
    """Настройка соединения с RabbitMQ и создание очереди."""
    try:
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@localhost:5672/",
            reconnect_interval=10,
            timeout=10
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("interactions_queue", durable=True)
        logger.info("RabbitMQ connection established, queue declared: %s", queue.name)
        return connection, channel, queue
    except Exception as e:
        logger.error(f"Failed to setup RabbitMQ: {e}", exc_info=True)
        raise

async def send_interaction(channel: aio_pika.Channel, interaction: Interaction):
    """Отправка взаимодействия в очередь."""
    try:
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
        logger.error(f"Failed to send interaction: {e}")
        raise