import pika
import time
import rpyc
import json
from addresses import *

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'queue_monitor_status_data_node'  # Nome da fila específica para este monitor
DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]


class MonitorStatus:
    def __init__(self):
        self.data_nodes = DATA_NODES_ADDR
        self.cluster_size = len(DATA_NODES_ADDR)
        self.status = {f"data_node_{i+1}": True for i in range(self.cluster_size)}
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        
        # Declara a fila única para este monitor
        self.channel.queue_declare(queue=QUEUE_NAME)
        self.offline_nodes = []


    def check_data_nodes(self):
        """Verifica se os Data Nodes estão online."""
        for i, (ip, port) in enumerate(self.data_nodes):
            node_id = f"data_node_{i+1}"
            try:
                conn = rpyc.connect(ip, port)
                conn.close()
                if not self.status[node_id]:
                    self.status[node_id] = True
                    if node_id in self.offline_nodes:
                        self.offline_nodes.remove(node_id)
                        self.notify_server(node_id, self.status[node_id])

            except ConnectionRefusedError:
                if self.status[node_id]:
                    self.status[node_id] = False
                    self.notify_server(node_id, self.status[node_id])
                    self.offline_nodes.append(node_id)


    def notify_server(self, node_id, status):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = {
            "node_id": node_id,
            "status": status
        }
        # Serializa o dicionário para uma string JSON
        message_json = json.dumps(message_dict)
        
        # Publica a string JSON na fila específica deste monitor
        self.channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message_json)
        print(f"[Monitor] Notificação enviada para {QUEUE_NAME}: {message_json}")


    def start_monitoring(self):
        """Inicia o monitoramento periódico."""
        print("[Monitor] Iniciando monitoramento dos Data Nodes...")
        try:
            while True:
                self.check_data_nodes()
                time.sleep(5)  # Intervalo entre as verificações
        except KeyboardInterrupt:
            print("[Monitor] Encerrando monitoramento...")
            self.connection.close()


if __name__ == "__main__":
    monitor = MonitorStatus()
    monitor.start_monitoring()