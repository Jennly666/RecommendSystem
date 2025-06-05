import asyncpg
import pandas as pd
import logging
from typing import List
from domain.entities import Video, Interaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VideoRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def get_all_videos(self) -> List[Video]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, genres FROM videos")
            videos = [Video(id=row["id"], genres=row["genres"]) for row in rows]
            logger.info(f"Retrieved {len(videos)} videos from database")
            return videos

    async def get_similarity_matrix(self) -> pd.DataFrame:
        async with self.pool.acquire() as conn:
            schema = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = 'video_similarity'")
            columns = [row["column_name"] for row in schema]
            logger.info(f"Columns in video_similarity table: {columns}")
            rows = await conn.fetch("SELECT video1_id, video2_id, similarity FROM video_similarity")
        if not rows:
            logger.warning("No similarity data found")
            return pd.DataFrame()
        data = {(r["video1_id"], r["video2_id"]): r["similarity"] for r in rows}
        video_ids = sorted(set(r["video1_id"] for r in rows) | set(r["video2_id"] for r in rows))
        matrix = pd.DataFrame(0.0, index=video_ids, columns=video_ids)
        for (v1, v2), sim in data.items():
            matrix.loc[v1, v2] = sim
            matrix.loc[v2, v1] = sim
        logger.debug("Loaded similarity matrix")
        return matrix

    async def save_similarity_matrix(self, matrix: pd.DataFrame):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM video_similarity")
                for v1 in matrix.index:
                    for v2 in matrix.columns:
                        if v1 <= v2 and matrix.loc[v1, v2] > 0:
                            await conn.execute(
                                "INSERT INTO video_similarity (video1_id, video2_id, similarity) VALUES ($1, $2, $3)",
                                v1, v2, float(matrix.loc[v1, v2])
                            )
            logger.debug("Saved similarity matrix")

class InteractionRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def get_all_interactions(self) -> List[Interaction]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, video_id, action FROM interactions")
            interactions = [Interaction(user_id=row["user_id"], video_id=row["video_id"], action=row["action"]) for row in rows]
            logger.debug(f"Retrieved {len(interactions)} interactions")
            return interactions

    async def save_interaction(self, interaction: Interaction):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO interactions (user_id, video_id, action) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                interaction.user_id, interaction.video_id, interaction.action
            )
            logger.debug(f"Saved interaction: {interaction.user_id}, {interaction.video_id}, {interaction.action})")