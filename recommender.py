# recommender.py
import numpy as np
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

# Видеокаталог
videos = {
    'video_1': {'genres': ['action', 'adventure']},
    'video_2': {'genres': ['comedy', 'romance']},
    'video_3': {'genres': ['action', 'sci-fi']},
    'video_4': {'genres': ['drama', 'romance']},
    'video_5': {'genres': ['comedy', 'action']},
    'video_6': {'genres': ['sci-fi', 'adventure']},
}

# Веса действий
action_weights = {
    'view': 1.0,
    'like': 2.0,
    'comment': 3.0,
    'favorite': 4.0
}

# Преобразование жанров в бинарные векторы
all_genres = set()
for video in videos.values():
    all_genres.update(video['genres'])
all_genres = list(all_genres)

def get_genre_vector(genres):
    return [1 if genre in genres else 0 for genre in all_genres]

# Глобальные переменные для матрицы и схожести
user_item_matrix = None
video_similarity = None

def build_user_item_matrix(interactions, min_interactions=2):
    global user_item_matrix
    user_interaction_count = defaultdict(int)
    for interaction in interactions:
        user_interaction_count[interaction['user']] += 1
    valid_users = [user for user, count in user_interaction_count.items() if count >= min_interactions]
    
    users = valid_users
    items = list(videos.keys())
    matrix = pd.DataFrame(0.0, index=users, columns=items)
    
    smoothing_factor = 0.1
    for interaction in interactions:
        if interaction['user'] in valid_users:
            video = interaction['video']
            action = interaction['action']
            score = action_weights.get(action, 0.0) + smoothing_factor
            matrix.loc[interaction['user'], video] += score
    
    user_item_matrix = matrix
    return matrix

def compute_video_similarity():
    global video_similarity
    video_ids = list(videos.keys())
    genre_matrix = np.array([get_genre_vector(videos[vid]['genres']) for vid in video_ids])
    similarity_matrix = cosine_similarity(genre_matrix)
    video_similarity = pd.DataFrame(similarity_matrix, index=video_ids, columns=video_ids)
    return video_similarity

def recommend_videos(user, top_n=3):
    if user_item_matrix is None or video_similarity is None:
        return []
    if user not in user_item_matrix.index:
        return []
    
    user_interactions = user_item_matrix.loc[user]
    watched_videos = user_interactions[user_interactions > 0].index
    
    scores = defaultdict(float)
    for video in videos:
        if video not in watched_videos:
            for watched_video in watched_videos:
                similarity = video_similarity.loc[video, watched_video]
                user_score = user_item_matrix.loc[user, watched_video]
                scores[video] += similarity * user_score
    
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_scores[:top_n]