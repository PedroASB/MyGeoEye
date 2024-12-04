import pika
import time
import rpyc
import json
from addresses import *

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'queue_monitor_score_data_node'  # Nome da fila específica para este monitor
DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
VERIFICATION_INTERVAL = 20 # Tempo em segundos entre cada verificação

STATUS_WEIGHTS = {"cpu": 0.3, "memory": 0.2, "disk": 0.5}

class MonitorScore:
    def __init__(self):
        self.data_nodes = DATA_NODES_ADDR
        self.cluster_size = len(DATA_NODES_ADDR)
        self.score_nodes = {f"data_node_{i+1}": None for i in range(self.cluster_size)}
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        
        # Declara a fila única para este monitor
        self.channel.queue_declare(queue=QUEUE_NAME)


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


    def notify_server(self):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = self.score_nodes
        # Serializa o dicionário para uma string JSON
        message_json = json.dumps(message_dict)
        
        # Publica a string JSON na fila específica deste monitor
        self.channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message_json)
        print(f"[INFO] Notificação enviada para {QUEUE_NAME}: {message_json}")


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


if __name__ == "__main__":
    monitor = MonitorScore()
    monitor.start_monitoring()
