import pika, time, json, threading
from addresses import *


RABBITMQ_HOST = 'localhost'

DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR, DATA_NODE_5_ADDR]
RECALCULATE_SCORE_INTERVAL = 20 # Tempo em segundos entre cada cálculo de score
STATUS_WEIGHTS = {"cpu": 0.3, "memory": 0.2, "disk": 0.5}


class Pub:
    EXCHANGE_MONITOR_DATA_NODE_SCORES = 'exchange_monitor_data_node_scores'
    QUEUE_MONITOR_DATA_NODE_SCORES = 'queue_monitor_data_node_scores'  # Nome da fila específica para este monitor

    def __init__(self, cluster_size, nodes_resources):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_SCORES, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_SCORES)
        self.nodes_scores = {f"data_node_{i+1}": None for i in range(cluster_size)}
        self.nodes_resources = nodes_resources


    def start_monitoring(self):
            """Inicia o monitoramento periódico."""
            print("[STATUS] Iniciando monitoramento dos data nodes.")
            try:
                while True:
                    self.calculate_scores()
                    self.notify_subs()
                    print('[INFO] Subscribers notificados.')
                    time.sleep(RECALCULATE_SCORE_INTERVAL)  # Intervalo entre as verificações
            except KeyboardInterrupt:
                print("[STATUS] Encerrando monitoramento.")
                self.connection.close()


    def calculate_scores(self):
        """Verifica o score de cada data node."""
        # nodes_resources[data_node_1] = {'cpu': ..., 'memory': ..., 'disk': ...}
        for node_id, resources in self.nodes_resources.items():
            cpu, memory, disk = resources.values()
            if not cpu or not memory or not disk:
                continue
            self.nodes_scores[node_id] = round((
                (100 - cpu)    * STATUS_WEIGHTS['cpu'] +
                (100 - memory) * STATUS_WEIGHTS['memory'] +
                (100 - disk)   * STATUS_WEIGHTS['disk']
            ), 3)


    def notify_subs(self):
        """Envia uma mensagem aos subscribers."""
        message_dict = self.nodes_scores
        message_json = json.dumps(message_dict)
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE_MONITOR_DATA_NODE_SCORES, body=message_json)
        print(f"[MONITOR_SCORE] Notificação enviada para {self.QUEUE_MONITOR_DATA_NODE_SCORES}: {message_json}")


class Sub:
    EXCHANGE_DATA_NODE_RESOURCES = 'exchange_data_node_resources'
    QUEUE_DATA_NODE_RESOURCES = 'queue_data_node_resources'

    def __init__(self, nodes_resources):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_DATA_NODE_RESOURCES, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_DATA_NODE_RESOURCES)
        self.channel.queue_bind(exchange=self.EXCHANGE_DATA_NODE_RESOURCES, queue=self.QUEUE_DATA_NODE_RESOURCES)
        self.nodes_resources = nodes_resources


    def start_listening(self):
        self.channel.basic_consume(queue=self.QUEUE_DATA_NODE_RESOURCES, on_message_callback=self.callback_data_nodes_resources, auto_ack=True)
        self.channel.start_consuming()


    def callback_data_nodes_resources(self, ch, method, properties, body):
        """Callback para processar mensagens do RabbitMQ."""
        message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
        print(f"[MONITOR_SCORE] Notificação recebida: {message_dict}")
        node_id = message_dict['node_id']
        self.nodes_resources[node_id]['cpu'] = message_dict['cpu']
        self.nodes_resources[node_id]['memory'] = message_dict['memory']
        self.nodes_resources[node_id]['disk'] = message_dict['disk']


class MonitorScore:
    def __init__(self):
        # self.data_nodes = DATA_NODES_ADDR
        self.cluster_size = len(DATA_NODES_ADDR)
        self.nodes_resources = {f"data_node_{i+1}": {'cpu': None, 'memory': None, 'disk': None} for i in range(self.cluster_size)}
        self.pub = Pub(self.cluster_size, self.nodes_resources)
        self.sub = Sub(self.nodes_resources)
        

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
    monitor = MonitorScore()
    monitor.start()
