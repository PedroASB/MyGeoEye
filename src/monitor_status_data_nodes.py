import pika, time, json, threading, rpyc
from copy import deepcopy
# from addresses import *

# DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR, DATA_NODE_5_ADDR]

RABBITMQ_HOST_SERVER = '192.168.40.117' # 'localhost'
RECALCULATE_STATUS_INTERVAL = 10
TIMEOUT = 15

HOST_NAME_SERVICE = '192.168.40.46' # 'localhost'
PORT_NAME_SERVICE = 6000

import pika
import json
import time
from copy import deepcopy

RECALCULATE_STATUS_INTERVAL = 5  # Ajuste conforme necessário
TIMEOUT = 10  # Ajuste conforme necessário
MAX_RETRIES = 5  # Número máximo de tentativas de reconexão

class Pub:
    EXCHANGE_MONITOR_DATA_NODE_STATUS = 'exchange_monitor_data_node_status'
    QUEUE_MONITOR_DATA_NODE_STATUS = 'queue_monitor_data_node_status'

    def __init__(self, nodes_status, lock):
        self.nodes_status = nodes_status
        self.lock = lock
        self.connection = None
        self.channel = None
        self._open_connection()  # Inicializa a conexão ao iniciar

    def _open_connection(self):
        """Abre uma nova conexão e um canal."""
        try:
            print("[MONITOR_STATUS] Abrindo conexão com RabbitMQ...")
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_STATUS, exchange_type='fanout')
            self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_STATUS)
            print("[MONITOR_STATUS] Conexão aberta com sucesso.")
        except Exception as e:
            print(f"[ERRO] Falha ao abrir conexão: {e}")
            self.connection = None
            self.channel = None

    def start_monitoring(self):
        """Inicia o monitoramento periódico."""
        print("[MONITOR_STATUS] Iniciando monitoramento dos Data Nodes...")
        try:
            while True:
                with self.lock:
                    changed_nodes = self.get_changed_nodes()
                    print(f"nodes_status: {self.nodes_status}")
                    print(f"changed_nodes: {changed_nodes}")
                    if changed_nodes:
                        self.notify_subs(changed_nodes)
                time.sleep(RECALCULATE_STATUS_INTERVAL)  # Intervalo entre as verificações
        except KeyboardInterrupt:
            print("[MONITOR_STATUS] Encerrando monitoramento.")
            self.close_connection()
        except Exception as e:
            print(f"[MONITOR_STATUS] Erro de monitoramento no Pub: {e}")
            self.close_connection()

    def notify_subs(self, changed_nodes):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = changed_nodes
        message_json = json.dumps(message_dict)
        retries = 0

        while retries < MAX_RETRIES:
            try:
                if not self.connection or self.connection.is_closed:
                    self._open_connection()  # Tenta abrir a conexão se estiver fechada
                self.channel.basic_publish(exchange='', routing_key=self.QUEUE_MONITOR_DATA_NODE_STATUS, body=message_json)
                print(f"[MONITOR_STATUS] Notificação enviada para {self.QUEUE_MONITOR_DATA_NODE_STATUS}: {message_json}")
                break  # Se a publicação foi bem-sucedida, sai do loop
            except Exception as e:
                print(f"[ERRO] Falha ao tentar publicar mensagem: {e}")
                self.close_connection()
                retries += 1
                if retries < MAX_RETRIES:
                    print(f"[ERRO] Tentando reconectar... (Tentativa {retries}/{MAX_RETRIES})")
                    time.sleep(2)  # Espera antes de tentar reconectar
                else:
                    print("[ERRO] Número máximo de tentativas de reconexão atingido.")
                    break  # Se as tentativas de reconexão falharem, encerra

    def get_changed_nodes(self):
        """Obtém os nós que mudaram de status (online/offline)."""
        changed_nodes = {}
        for node_id, previous_status in deepcopy(self.nodes_status).items():
            if previous_status['time'] is None:
                continue
            # Data node não está ativo agora
            if time.time() - previous_status['time'] >= TIMEOUT:
                self.nodes_status[node_id]['online'] = False
                if previous_status['online'] is True:  # Estava online antes
                    changed_nodes[node_id] = self.nodes_status[node_id]['online']
            # Data node está ativo agora
            else:
                self.nodes_status[node_id]['online'] = True
                if previous_status['online'] is False:  # Estava offline antes
                    changed_nodes[node_id] = self.nodes_status[node_id]['online']
        return changed_nodes

    def close_connection(self):
        """Fecha a conexão e o canal."""
        if self.channel and not self.channel.is_closed:
            self.channel.close()
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        print("[MONITOR_STATUS] Conexão fechada.")
        

class Sub:
    EXCHANGE_DATA_NODE_STATUS = 'exchange_data_node_status'
    QUEUE_DATA_NODE_STATUS = 'queue_data_node_status'  # Nome da fila específica para este data node

    def __init__(self, nodes_status, lock):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_DATA_NODE_STATUS)
        self.channel.queue_bind(exchange=self.EXCHANGE_DATA_NODE_STATUS, queue=self.QUEUE_DATA_NODE_STATUS)
        self.nodes_status = nodes_status
        self.lock = lock


    def start_listening(self):
        try:
            self.channel.basic_consume(queue=self.QUEUE_DATA_NODE_STATUS, on_message_callback=self.callback_data_status, auto_ack=True)
            self.channel.start_consuming()
        except Exception:
            print("[MONITOR_STATUS] Erro de monitoramento no Sub.")


    def callback_data_status(self, ch, method, properties, body):
        """Callback para processar mensagens do RabbitMQ."""
        message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
        node_id = message_dict['node_id']
        with self.lock:
            self.nodes_status[node_id]['time'] = message_dict['time']
        print(f"[MONITOR_STATUS] Notificação recebida: {message_dict}")


class MonitorStatus:
    def __init__(self):
        self.name_service_conn = rpyc.connect(HOST_NAME_SERVICE, PORT_NAME_SERVICE)
        self.data_nodes_addresses = self.name_service_conn.root.lookup_data_nodes()
        self.cluster_size = len(self.data_nodes_addresses)
        self.nodes_status = {f"data_node_{i+1}": {'online': False, 'time': None} for i in range(self.cluster_size)}

        self.lock = threading.Lock()

        self.pub = Pub(self.nodes_status, self.lock)
        self.sub = Sub(self.nodes_status, self.lock)


    def start(self):
        pub_thread = threading.Thread(target=self.pub.start_monitoring, daemon=True)
        sub_thread = threading.Thread(target=self.sub.start_listening, daemon=True)

        # Inicia as threads
        pub_thread.start()
        sub_thread.start()

        try:
            while pub_thread.is_alive() or sub_thread.is_alive():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("[KEYBOARD_INTERRUPT] Encerrando as threads.")


if __name__ == "__main__":
    monitor = MonitorStatus()
    monitor.start()
