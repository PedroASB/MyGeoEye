import rpyc, time, pika, json, threading

RABBITMQ_HOST = 'localhost'
BASE_SCORE = 42

class PubNodesServer:
    EXCHANGE_MONITOR_DATA_NODE_STATUS = 'exchange_monitor_data_node_status'
    QUEUE_MONITOR_DATA_NODE_STATUS = 'queue_monitor_data_node_status'

    def __init__(self, nodes_status, lock):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_STATUS)
        self.nodes_status = nodes_status
        self.lock = lock

    def notify_subs(self, changed_nodes):
        """Envia uma mensagem ao servidor notificando o status do Data Node."""
        message_dict = changed_nodes
        message_json = json.dumps(message_dict)
        self.channel.basic_publish(exchange='', routing_key=self.QUEUE_MONITOR_DATA_NODE_STATUS, body=message_json)
        print(f"[Monitor] Notificação enviada para {self.QUEUE_MONITOR_DATA_NODE_STATUS}: {message_json}")


class SubScore:
    EXCHANGE_MONITOR_DATA_NODE_SCORES = 'exchange_monitor_data_node_scores'
    QUEUE_MONITOR_DATA_NODE_SCORES = 'queue_monitor_data_node_scores'

    def __init__(self, data_nodes, lock):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_SCORES, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_SCORES)
        self.channel.queue_bind(exchange=self.EXCHANGE_MONITOR_DATA_NODE_SCORES, queue=self.QUEUE_MONITOR_DATA_NODE_SCORES)
        self.data_nodes = data_nodes
        self.lock = lock


    def callback_data_nodes_scores(self, ch, method, properties, body):
        """Callback para processar mensagens do RabbitMQ."""
        message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
        print(f"[MONITOR_SCORE] Notificação recebida: {message_dict}")
        # Acessar informações
        with self.lock:
            for node_id, score in message_dict.items():
                if node_id not in self.data_nodes:
                    continue
                self.data_nodes[node_id]['score'] = score    

    def start_listening_sub_score(self):
        self.channel.basic_consume(queue=self.QUEUE_MONITOR_DATA_NODE_SCORES, on_message_callback=self.callback_data_nodes_scores, auto_ack=True)
        self.channel.start_consuming()


class SubStatus:
    EXCHANGE_MONITOR_DATA_NODE_STATUS = 'exchange_monitor_data_node_status'
    QUEUE_MONITOR_DATA_NODE_STATUS = 'queue_monitor_data_node_status'

    def __init__(self, data_nodes, lock, handle_offline_node):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_MONITOR_DATA_NODE_STATUS, exchange_type='fanout')
        self.channel.queue_declare(queue=self.QUEUE_MONITOR_DATA_NODE_STATUS)
        self.channel.queue_bind(exchange=self.EXCHANGE_MONITOR_DATA_NODE_STATUS, queue=self.QUEUE_MONITOR_DATA_NODE_STATUS)
        self.data_nodes = data_nodes
        self.handle_offline_node = handle_offline_node
        self.lock = lock
    

    def callback_data_node_status(self, ch, method, properties, body):
        """Callback para processar mensagens do RabbitMQ."""
        message_dict = json.loads(body)  # Converte a string JSON de volta para um dicionário
        print(f"[MONITOR_STATUS] Notificação recebida: {message_dict}")
        # Acessar informações
        with self.lock:
            for node_id, is_online in message_dict.items():
                if node_id not in self.data_nodes:
                    continue
                self.data_nodes[node_id]['online'] = is_online
                # server.name:AttributeError
                if is_online is False:
                    self.handle_offline_node(node_id)


    def start_listening_sub_status(self):
        self.channel.basic_consume(queue=self.QUEUE_MONITOR_DATA_NODE_STATUS, 
                                   on_message_callback=self.callback_data_node_status, auto_ack=True)
        self.channel.start_consuming()


class Cluster:
    def __init__(self, cluster_size, replication_factor, name_service_conn):
        self.cluster_size = cluster_size
        self.data_nodes_addresses = name_service_conn.root.lookup_data_nodes(quantity=cluster_size)
        print('Data nodes:')
        print(self.data_nodes_addresses)
        self.replication_factor = replication_factor
        self.name_service_conn = name_service_conn
        self.current_node_to_store = 1
        self.index_table = {}
        # TODO: arrumar índices dos data nodes
        self.data_nodes = {node_id: {
                                    'addr': address, 
                                    'conn': None, 
                                    'online': None,
                                    'score': None,
                                    }
                                    for node_id, address in self.data_nodes_addresses.items()}
        self.lock = threading.Lock()
        self.sub_score = PubNodesServer(self.data_nodes, self.lock)
        self.sub_score = SubScore(self.data_nodes, self.lock)
        self.sub_status = SubStatus(self.data_nodes, self.lock, self.handle_offline_node)
    

    def exposed_get_data_nodes_addresses(self):
        return self.data_nodes_addresses


    def connect_cluster(self):
        """Estabelece conexão com todos os data nodes"""
        for key, value in self.data_nodes.items():
            self.data_nodes[key]['conn'] = self.connect_data_node(value['addr'])
            self.data_nodes[key]['online'] = True
        print('[STATUS] Todos os data nodes do cluster foram conectados com sucesso.')


    def connect_data_node(self, address, persistent=True):
        data_node_conn = None
        ip, port = address[0], address[1]
        while not data_node_conn:
            try:
                print(f"[STATUS] Tentando conectar ao data node em {ip}:{port}.")
                data_node_conn = rpyc.connect(ip, port)
                print(f"[STATUS] Conexão com o data node estabelecida em {ip}:{port}.")
                # TODO: tirar isso:
                data_node_conn.root.clear_storage_dir()
            except ConnectionRefusedError:
                if not persistent:
                    return None
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos.")
                data_node_conn = None
                time.sleep(5)
        return data_node_conn
    
    
    def select_nodes_to_store(self):
        """
        Seleciona os data nodes para armazenamento de imagens
        Utiliza-se balanceamento de carga pelos recursos de máquina
        """
        scored_nodes = [node_id for node_id in self.data_nodes if \
                        self.data_nodes[node_id]['online'] and \
                        None != self.data_nodes[node_id]['score'] >= BASE_SCORE]
        scored_nodes.sort(key=lambda x: x[1], reverse=True)
        storage_nodes = [node_id for node_id in scored_nodes]
        if len(storage_nodes) < self.replication_factor:
            storage_nodes = [node_id for node_id, _ in scored_nodes[:self.replication_factor]]
        print(f'[INFO] Data nodes selecionados para armazenamento: {storage_nodes}')
        return storage_nodes


    def select_nodes_to_retrieve(self, image_name):
        """
        Seleciona os data nodes para armazenamento de imagens
        Utiliza-se balanceamento de carga pelos recursos de máquina
        """
        # img_01  ->  [[part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        retrieval_nodes = []
        for shard in self.index_table[image_name]:
            # shard => {'nodes': 3 2 4, 'size': 100}
            # sorted([(id, 5), (id, 3), (id, 2)])
            scored_nodes = [
                node_id for node_id, _ in sorted(
                    ((node_id, self.data_nodes[node_id]['score']) for node_id in shard['nodes']),
                    key=lambda x: x[1],
                    reverse=True
                )
            ]
            retrieval_nodes.append(scored_nodes[0])
        print(f'\nretrieval_nodes to "{image_name}" =', retrieval_nodes)
        return retrieval_nodes
    

    def image_total_size(self, image_name):
        total_size = 0
        for shard in self.index_table[image_name]:
            total_size += shard['size']
        return total_size


    def init_update_index_table(self, image_name, image_size_division):
        if image_name in self.index_table:
            return False
        # img_01  ->  [[part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        self.index_table[image_name] = [{'nodes': [], 'size': None} \
                                        for _ in range(image_size_division)]
        return True
            

    def update_index_table(self, image_name, shard_index, shard_size, nodes_id):
        """Atualiza a tabela de índices para uma imagem dividida em partes."""
        # img_01  ->  [part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        for node_id in nodes_id:
            if node_id not in self.index_table[image_name][shard_index]:
                self.index_table[image_name][shard_index]['nodes'].append(node_id)
        self.index_table[image_name][shard_index]['size'] = shard_size
        # print('index_table:')
        # for k, v in self.index_table.items():
        #     print(f'{k}: {v}')
        # print()


    def connect_new_data_node(self, old_data_node_id):
        available_data_nodes = self.name_service_conn.root.lookup_data_nodes()
        del self.data_nodes[old_data_node_id]
        new_node_conn = None
        while new_node_conn is None:
            for node_id, addr in available_data_nodes.items():
                if node_id not in self.data_nodes:
                    print(f'[STATUS] Recuperação pós falha: tentando conectar ao data node "{node_id}"')
                    new_node_conn = self.connect_data_node(address=addr, persistent=False)
                    if new_node_conn:
                        self.data_nodes[node_id] = {}
                        self.data_nodes[node_id]['addr'] = addr
                        self.data_nodes[node_id]['conn'] = new_node_conn
                        self.data_nodes[node_id]['online'] = True
                        self.data_nodes[node_id]['score'] = None
                        return node_id
        return None
                

    def new_storage(self, source_nodes_ids, new_node_id, image_shard_name):
        store_node = self.data_nodes[new_node_id]
        retrieval_node = self.data_nodes[source_nodes_ids[0]]

        print('new_node_id =', new_node_id, '\n')
        print('source_nodes_ids =', source_nodes_ids)
        print('retrieval_node_id =', source_nodes_ids[0])

        image_shard = retrieval_node['conn'].root.retrieve_image_shard(image_shard_name)
        store_node['conn'].root.store_image_shard(image_shard_name, image_shard)

    
    def handle_offline_node(self, offline_node_id):
        new_node_id = self.connect_new_data_node(offline_node_id)
        # img_01  ->  [part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        for image_name, registry in self.index_table.items():
            # img_0 -> [part_0: {'nodes': 3 2 4, 'size': 100}, ...]
            # img_1 -> [part_0: {'nodes': 1 9 4, 'size': 100}, ...]
            # img_2 -> [part_0: {'nodes': 7 8 3, 'size': 100}, ...]
            for shard_index, shard in enumerate(registry):
                # [part_0: {'nodes': 7 8 3, 'size': 100}, ...]
                if offline_node_id in shard['nodes']:
                    # 'nodes:' [2 3 4* 5]
                    shard['nodes'].remove(offline_node_id)
                    source_nodes_ids = shard['nodes'][:]
                    image_shard_name = f'{image_name}%part{shard_index}%'
                    print('image_shard_name =', image_shard_name)
                    self.new_storage(source_nodes_ids, new_node_id, image_shard_name)
                    shard['nodes'].append(new_node_id)