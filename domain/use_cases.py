import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from domain.entities import Video, Interaction
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Recommender:
    def __init__(self, videos: List[Video], action_weights: Dict[str, float]):
        self.videos = {v.id: v for v in videos}
        self.video_ids = list(self.videos.keys())
        self.action_weights = action_weights
        self.user_item_matrix = None
        self.video_similarity = None
        logger.info("Recommender initialized with %d videos", len(self.videos))

    def compute_video_similarity(self):
        """Вычисление матрицы схожести видео на основе жанров."""
        try:
            if not self.videos:
                logger.error("No videos available for similarity computation")
                raise ValueError("Empty video list")

            mlb = MultiLabelBinarizer()
            genres = [v.genres or [] for v in self.videos.values()]
            if not any(genres):
                logger.error("No genres available for any video")
                raise ValueError("No valid genres")

            genre_matrix = mlb.fit_transform(genres)
            logger.debug("Genre matrix shape: %s, classes: %s", genre_matrix.shape, mlb.classes_)

            similarity = cosine_similarity(genre_matrix)
            # Add small noise to avoid uniform similarities
            similarity += np.random.normal(0, 0.01, similarity.shape)
            np.fill_diagonal(similarity, 1.0)  # Ensure self-similarity is 1
            self.video_similarity = pd.DataFrame(
                similarity,
                index=self.video_ids,
                columns=self.video_ids
            )
            logger.info("Computed video similarity matrix with shape: %s", self.video_similarity.shape)
        except Exception as e:
            logger.error(f"Error computing video similarity: {e}", exc_info=True)
            raise

    def update_user_item_matrix(self, interactions: List[Interaction], user_id: str = None) -> pd.DataFrame:
        """Обновление матрицы пользователь-элемент."""
        try:
            smoothing_factor = 0.1
            valid_users = set(i.user_id for i in interactions if i.video_id in self.video_ids)
            logger.debug(f"Valid users: {valid_users}, target user_id: {user_id}")
            if not valid_users:
                logger.warning("No valid users or interactions for matrix update")
                return self.user_item_matrix

            users = [user_id] if user_id else list(valid_users)
            items = self.video_ids

            if self.user_item_matrix is None:
                matrix = pd.DataFrame(0.0, index=users, columns=items)
            else:
                matrix = self.user_item_matrix.copy()
                new_users = [u for u in users if u not in matrix.index]
                if new_users:
                    logger.debug("Adding new users: %s", new_users)
                    new_rows = pd.DataFrame(0.0, index=new_users, columns=items)
                    matrix = pd.concat([matrix, new_rows])
                if user_id and user_id in matrix.index:
                    matrix.loc[user_id] = 0.0

            for interaction in interactions:
                if interaction.user_id in valid_users and interaction.video_id in self.video_ids:
                    score = self.action_weights.get(interaction.action, 0.0) + smoothing_factor
                    try:
                        matrix.loc[interaction.user_id, interaction.video_id] += score
                    except KeyError as e:
                        logger.error(f"KeyError in matrix update: {e}, user_id: {interaction.user_id}, video_id: {interaction.video_id}")
                        continue

            self.user_item_matrix = matrix
            logger.info(f"User-item matrix updated with shape: {matrix.shape}, users: {matrix.index.tolist()}")
            return matrix
        except Exception as e:
            logger.error(f"Error updating user-item matrix: {e}", exc_info=True)
            raise

    def recommend(self, user_id: str, n: int = 3) -> List[Tuple[str, float]]:
        """Генерация рекомендаций для пользователя."""
        try:
            logger.info(f"Generating recommendations for user_id: {user_id}")
            if self.user_item_matrix is None or user_id not in self.user_item_matrix.index:
                logger.warning(f"User {user_id} not found in user_item_matrix")
                return [(vid, 0.0) for vid in self.video_ids[:n]]

            if self.video_similarity is None:
                logger.warning("Video similarity matrix is not initialized")
                return [(vid, 0.0) for vid in self.video_ids[:n]]

            user_ratings = self.user_item_matrix.loc[user_id]
            scores = self.video_similarity.dot(user_ratings)
            # Normalize scores to improve diversity
            scores = (scores - scores.min()) / (scores.max() - scores.min() + 1e-8)
            scores = scores.sort_values(ascending=False)
            recommended = [(vid, float(score)) for vid, score in scores.items()
                           if user_ratings.get(vid, 0) == 0][:n]
            
            if not recommended:
                logger.warning(f"No recommendations for user {user_id}; falling back to popular videos")
                recommended = [(vid, 0.0) for vid in self.video_ids[:n]]

            logger.info(f"Generated {len(recommended)} recommendations for {user_id}: {recommended}")
            return recommended
        except Exception as e:
            logger.error(f"Error generating recommendations for user {user_id}: {e}", exc_info=True)
            return [(vid, 0.0) for vid in self.video_ids[:n]]