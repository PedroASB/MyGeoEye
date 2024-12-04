import pika
import time
import rpyc
import json
import threading
from addresses import *


RABBITMQ_HOST = 'localhost'
EXCHANGE_NAME = 'datanode_status'
QUEUE_MONITOR_SCORE_DATA_NODE = 'queue_monitor_score_data_node'  # Nome da fila específica para este monitor
QUEUE_STATUS_DATA_NODE = 'queue_status_data_node'  # Nome da fila específica para este monitor

DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
VERIFICATION_INTERVAL = 20 # Tempo em segundos entre cada verificação

STATUS_WEIGHTS = {"cpu": 0.3, "memory": 0.2, "disk": 0.5}


class Pub:
    def __init__(self, cluster_size):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=QUEUE_MONITOR_SCORE_DATA_NODE)
        self.score_nodes = {f"data_node_{i+1}": None for i in range(cluster_size)}


    def start_monitoring(self):
            """Inicia o monitoramento periódico."""
            print("[STATUS] Iniciando monitoramento dos data nodes.")
            try:
                while True:
                    self.check_score_data_nodes()
                    time.sleep(VERIFICATION_INTERVAL)  # Intervalo entre as verificações
            except KeyboardInterrupt:
                print("[STATUS] Encerrando monitoramento.")
                self.connection.close()


    def check_score_data_nodes(self):
        """Verifica o score de cada data node."""
        for i, (ip, port) in enumerate(self.data_nodes):
            node_id = f"data_node_{i+1}"
            try:
                conn = rpyc.connect(ip, port)
                self.score_nodes[node_id] = self.calculate_score(node_id, conn)
                conn.close()

            except ConnectionRefusedError:
                self.score_nodes[node_id] = None
        self.notify_server()


    def calculate_score(self, node_id, data_node_conn):
        """Função para calcular o score com base nos recursos disponíveis"""
        node_resources = data_node_conn.root.get_node_status()
        cpu, memory, disk = list(node_resources.values())
        score = round((
            (100 - cpu)    * STATUS_WEIGHTS['cpu'] +
            (100 - memory) * STATUS_WEIGHTS['memory'] +
            (100 - disk)   * STATUS_WEIGHTS['disk']
        ), 3)
        print(f'[INFO] Score do node "{node_id}": {score}')
        return score


    def notify_server(self):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = self.score_nodes
        message_json = json.dumps(message_dict)
        
        self.channel.basic_publish(exchange='', routing_key=QUEUE_MONITOR_SCORE_DATA_NODE, body=message_json)
        print(f"[INFO] Notificação enviada para {QUEUE_MONITOR_SCORE_DATA_NODE}: {message_json}")


class Sub:
    def __init__(self, cluster_size):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=QUEUE_STATUS_DATA_NODE)
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_STATUS_DATA_NODE)
        self.status_nodes = {f"data_node_{i+1}": {'cpu': None, 'memory': None, 'disk': None} for i in range(cluster_size)}


    def start_listening(self):
        self.channel.basic_consume(queue=QUEUE_STATUS_DATA_NODE, on_message_callback=self.callback_status_data_node, auto_ack=True)
        self.channel.start_consuming()


    def callback_status_data_node(self, ch, method, properties, body):
            """Callback para processar mensagens do RabbitMQ."""
            message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
            print(f"[MONITOR] Notificação recebida: {message_dict}")
            node_id = message_dict['node_id']
            self.status_nodes[node_id]['cpu'] = message_dict['cpu']
            self.status_nodes[node_id]['memory'] = message_dict['memory']
            self.status_nodes[node_id]['disk'] = message_dict['disk']


class MonitorScore:
    def __init__(self):
        self.data_nodes = DATA_NODES_ADDR
        self.cluster_size = len(DATA_NODES_ADDR)

        self.sub = Sub(self.cluster_size)
        self.pub = Pub(self.cluster_size)
        

    def start(self):
        monitor_score_thread = threading.Thread(target=self.start_monitor_score_data_node)
        status_data_node_thread = threading.Thread(target=self.start_listening)

        # Inicia as threads
        monitor_score_thread.start()
        status_data_node_thread.start()

        # Aguarda as threads finalizarem
        monitor_score_thread.join()
        status_data_node_thread.join()


    def start_monitor_score_data_node(self):
        self.start_monitoring()

if __name__ == "__main__":
    monitor = MonitorScore()
    monitor.start()
