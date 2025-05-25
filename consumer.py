# consumer.py
import pika
import json
from recommender import build_user_item_matrix, compute_video_similarity, recommend_videos, videos

def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='interactions_queue', durable=True)
    return connection, channel

def consume_interactions(channel, interactions):
    def callback(ch, method, properties, body):
        interaction = json.loads(body)
        print(f"Получено взаимодействие: {interaction}")
        interactions.append(interaction)
        
        # Обновляем матрицу пользователь-элемент
        build_user_item_matrix(interactions)
        
        # Генерируем рекомендации
        user = interaction['user']
        recommendations = recommend_videos(user)
        print(f"Обновленные рекомендации для {user}:")
        for video, score in recommendations:
            print(f"{video}: {videos[video]['genres']}, оценка {score:.2f}")
        print("---")

    channel.basic_consume(queue='interactions_queue', on_message_callback=callback, auto_ack=True)
    print("Потребитель запущен, ожидание сообщений...")
    channel.start_consuming()

if __name__ == "__main__":
    # Инициализация данных
    interactions = [
        {'user': 'user_1', 'video': 'video_1', 'action': 'view'},
        {'user': 'user_1', 'video': 'video_1', 'action': 'like'},
        {'user': 'user_1', 'video': 'video_3', 'action': 'view'},
        {'user': 'user_1', 'video': 'video_5', 'action': 'comment'},
        {'user': 'user_2', 'video': 'video_2', 'action': 'view'},
        {'user': 'user_2', 'video': 'video_2', 'action': 'favorite'},
        {'user': 'user_2', 'video': 'video_4', 'action': 'view'},
    ]
    
    # Инициализация матрицы и схожести
    build_user_item_matrix(interactions)
    compute_video_similarity()
    
    # Запуск потребителя
    connection, channel = setup_rabbitmq()
    try:
        consume_interactions(channel, interactions)
    except KeyboardInterrupt:
        connection.close()
        print("Потребитель завершен")