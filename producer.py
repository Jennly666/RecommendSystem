# producer.py
import pika
import json
import time

def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # Объявляем стандартную очередь (не stream)
    channel.queue_declare(queue='interactions_queue', durable=True)
    return connection, channel

def send_interaction(channel, interaction):
    channel.basic_publish(
        exchange='',
        routing_key='interactions_queue',
        body=json.dumps(interaction),
        properties=pika.BasicProperties(delivery_mode=2)  # Долговременные сообщения
    )
    print(f"Отправлено взаимодействие: {interaction}")

if __name__ == "__main__":
    connection, channel = setup_rabbitmq()
    
    # Симуляция новых взаимодействий
    new_interactions = [
        {'user': 'user_1', 'video': 'video_6', 'action': 'view'},
        {'user': 'user_1', 'video': 'video_6', 'action': 'favorite'},
        {'user': 'user_2', 'video': 'video_3', 'action': 'like'},
    ]
    
    # Отправка взаимодействий
    for interaction in new_interactions:
        send_interaction(channel, interaction)
        time.sleep(1)  # Задержка для имитации реального времени
    
    connection.close()