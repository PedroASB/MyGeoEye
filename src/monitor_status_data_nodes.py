import pika
import time
import rpyc
import json
import threading
from addresses import *

RABBITMQ_HOST = 'localhost'
DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
RECALCULATE_STATUS_INTERVAL = 10 # Tempo em segundos entre cada cálculo de score
TIMEOUT = 25

class Pub:
    EXCHANGE_MONITOR_DATA_NODE_STATUS = 'exchange_monitor_data_node_status'
    QUEUE_MONITOR_DATA_NODE_STATUS = 'queue_monitor_data_node_status'

    def __init__(self, nodes_status):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_STATUS)
        self.nodes_status = nodes_status

    def start_monitoring(self):
        """Inicia o monitoramento periódico."""
        print("[Monitor] Iniciando monitoramento dos Data Nodes...")
        try:
            while True:
                for node_id, status in self.nodes_status.copy().items():
                    if status['online'] is None or status['time'] is None:
                        continue
                    if time.time() - status['time'] >= TIMEOUT: # Data não está ativo agora, ...
                        if status['online'] is True: # ...mas estava online antes
                            self.nodes_status[node_id]['change'] = True
                        self.nodes_status[node_id]['online'] = False
                    else: # Data node está ativo agora, ...
                        if status['online'] is False: # ...mas estava offline antes
                            self.nodes_status[node_id]['change'] = True
                        self.nodes_status[node_id]['online'] = True
                    self.notify_subs()
                time.sleep(RECALCULATE_STATUS_INTERVAL)  # Intervalo entre as verificações
        except KeyboardInterrupt:
            print("[Monitor] Encerrando monitoramento...")
            self.connection.close()


    def notify_subs(self):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = {}
        print(f'nodes_status = {self.nodes_status}')
        for node_id, status in self.nodes_status.copy().items():
            if status['change'] is True:
                message_dict[node_id] = status['online']
                self.nodes_status[node_id]['change'] = False
                
        # Serializa o dicionário para uma string JSON
        message_json = json.dumps(message_dict)
        
        # Publica a string JSON na fila específica deste monitor
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE_MONITOR_DATA_NODE_STATUS, body=message_json)
        print(f"[Monitor] Notificação enviada para {self.QUEUE_MONITOR_DATA_NODE_STATUS}: {message_json}")


class Sub:
    EXCHANGE_DATA_NODE_STATUS = 'exchange_data_node_status'
    QUEUE_DATA_NODE_STATUS = 'queue_data_node_status'  # Nome da fila específica para este data node

    def __init__(self, nodes_status):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_DATA_NODE_STATUS)
        self.channel.queue_bind(exchange=self.EXCHANGE_DATA_NODE_STATUS, queue=self.QUEUE_DATA_NODE_STATUS)
        self.nodes_status = nodes_status


    def start_listening(self):
        self.channel.basic_consume(queue=self.QUEUE_DATA_NODE_STATUS, on_message_callback=self.callback_data_status, auto_ack=True)
        self.channel.start_consuming()


    def callback_data_status(self, ch, method, properties, body):
        """Callback para processar mensagens do RabbitMQ."""
        message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
        node_id = message_dict['node_id']
        self.nodes_status[node_id]['online'] = True
        self.nodes_status[node_id]['time'] = time.time()


class MonitorStatus:
    def __init__(self):
        self.cluster_size = len(DATA_NODES_ADDR)
        self.nodes_status = {f"data_node_{i+1}": {'online': None, 'time': None, 'change': False} for i in range(self.cluster_size)}
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()

        self.pub = Pub(self.nodes_status)
        self.sub = Sub(self.nodes_status)

    def start(self):
        pub_thread = threading.Thread(target=self.pub.start_monitoring, daemon=True)
        sub_thread = threading.Thread(target=self.sub.start_listening, daemon=True)

        # Inicia as threads
        pub_thread.start()
        sub_thread.start()

        # Aguarda as threads finalizarem
        # pub_thread.join()
        # sub_thread.join()

        try:
            while pub_thread.is_alive() or \
                sub_thread.is_alive():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("[KEYBOARD_INTERRUPT] Encerrando as threads.")


if __name__ == "__main__":
    monitor = MonitorStatus()
    monitor.start()
