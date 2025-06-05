from fastapi import FastAPI, HTTPException, Depends
from domain.use_cases import Recommender
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.get("/recommendations/{user_id}")
async def get_recommendations(
    user_id: str,
    recommender: Recommender = Depends(lambda: app.state.recommender)
):
    """Получение рекомендаций для указанного пользователя."""
    logger.info(f"Received request for recommendations for user_id: {user_id}")
    recommendations = recommender.recommend(user_id)
    if not recommendations:
        logger.warning(f"No recommendations available for user_id: {user_id}")
        raise HTTPException(status_code=404, detail="No recommendations available")
    logger.info(f"Returning recommendations: {recommendations}")
    return [{"video_id": vid, "score": score} for vid, score in recommendations]