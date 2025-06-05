import aio_pika
from domain.entities import Interaction
from domain.use_cases import Recommender
from infrastructure.db import InteractionRepository
import json
import logging
import asyncio

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def consume_interactions(queue: aio_pika.Queue, recommender: Recommender, interaction_repo: InteractionRepository):
    logger.info("Starting RabbitMQ consumer...")
    while True:
        try:
            async for message in queue:
                async with message.process(ignore_processed=True):
                    try:
                        body = message.body.decode()
                        logger.debug(f"Received message: {body}")
                        data = json.loads(body)
                        interaction = Interaction(
                            user_id=data["user_id"],
                            video_id=data["video_id"],
                            action=data["action"],
                        )
                        logger.info(f"Processing interaction: {interaction}")
                        await interaction_repo.save_interaction(interaction)
                        interactions = await interaction_repo.get_all_interactions()
                        logger.info(f"Updating user-item matrix for user {interaction.user_id}")
                        recommender.update_user_item_matrix(interactions, user_id=interaction.user_id)
                        recommendations = recommender.recommend(interaction.user_id)
                        logger.debug(f"Recommendations for {interaction.user_id}: {recommendations}")
                        await message.ack()
                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)
                        await message.nack(requeue=True)
                        await asyncio.sleep(1)
        except (aio_pika.exceptions.AMQPConnectionError, asyncio.CancelledError) as e:
            logger.warning(f"Consumer interrupted: {e}; reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected consumer error: {e}", exc_info=True)
            await asyncio.sleep(5)