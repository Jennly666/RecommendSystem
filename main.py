import asyncio
import asyncpg
from domain.use_cases import Recommender
from infrastructure.db import VideoRepository, InteractionRepository
from infrastructure.rabbitmq import setup_rabbitmq
from interfaces.api import app
from interfaces.consumer import consume_interactions
import logging
from uvicorn import Config, Server
from config import host, user, password, db_name

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def init_db():
    try:
        pool = await asyncpg.create_pool(
            user=user,
            password=password,
            database=db_name,
            host=host,
            min_size=1,
            max_size=10
        )
        logger.info("Database pool initialized successfully")
        return pool
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def main():
    try:
        pool = await init_db()
        video_repo = VideoRepository(pool)
        interaction_repo = InteractionRepository(pool)

        logger.info("Loading videos")
        videos = await video_repo.get_all_videos()
        if not videos:
            logger.error("No videos found in database")
            raise ValueError("Empty video list")

        action_weights = {"view": 1.0, "like": 2.0, "comment": 3.0, "favorite": 4.0}
        recommender = Recommender(videos, action_weights)
        
        logger.info("Checking similarity matrix")
        similarity_matrix = await video_repo.get_similarity_matrix()
        if not similarity_matrix.empty:
            logger.info("Loading existing similarity matrix")
            recommender.video_similarity = similarity_matrix
        else:
            logger.info("Computing new similarity matrix")
            recommender.compute_video_similarity()
            await video_repo.save_similarity_matrix(recommender.video_similarity)

        logger.info("Initializing user-item matrix")
        interactions = await interaction_repo.get_all_interactions()
        recommender.update_user_item_matrix(interactions)

        app.state.recommender = recommender
        app.state.interaction_repo = interaction_repo
        app.state.db_pool = pool

        logger.info("Setting up RabbitMQ")
        connection, channel, queue = await setup_rabbitmq()
        app.state.rabbitmq_connection = connection
        consumer_task = asyncio.create_task(consume_interactions(queue, recommender, interaction_repo))
        logger.info("Consumer task started")

        # Run Uvicorn server in the same event loop
        config = Config(app=app, host="0.0.0.0", port=8000, log_level="info")
        server = Server(config)
        await server.serve()

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise
    finally:
        # Cleanup
        if hasattr(app.state, "rabbitmq_connection"):
            await app.state.rabbitmq_connection.close()
        if hasattr(app.state, "db_pool"):
            await app.state.db_pool.close()
        if "consumer_task" in locals():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    asyncio.run(main())