import rpyc
import time

STATUS_WEIGHTS = {"cpu": 0.3, "memory": 0.2, "disk": 0.5}
BASE_SCORE = 50

class Cluster:

    def __init__(self, data_nodes_addresses, replication_factor, division_factor):
        self.data_nodes_addresses = data_nodes_addresses
        self.replication_factor = replication_factor
        self.division_factor = division_factor
        # self.data_nodes = {f'data_node_{i+1}': [self.data_nodes[i], None] for i in range(self.cluster_size)}
        self.cluster_size = len(data_nodes_addresses)
        self.current_node_to_store = 1
        self.index_table = {}
        self.data_nodes = {f'data_node_{i+1}': {
                                                'addr': self.data_nodes_addresses[i], 
                                                'conn': None, 
                                                'status': {
                                                    'cpu': None, 
                                                    'memory': None, 
                                                    'disk': None
                                                    }
                                                }
                                                for i in range(self.cluster_size)}


    def connect_cluster(self):
        """Estabelece conexão com todos os data nodes"""
        for key, value in self.data_nodes.items():
            # key: data_node_i
            # value: ((IP, PORT), socket)
            # self.data_node_id['data_node_3'][1] = self.connect_data_node(('1.1.1.1', '8003'))
            self.data_nodes[key]['conn'] = self.connect_data_node(value['addr'])
        print('[STATUS] Todos os data nodes do cluster foram conectados com sucesso.')


    def connect_data_node(self, address):
        data_node_conn = None
        ip, port = address[0], address[1]
        while not data_node_conn:
            try:
                print(f"[STATUS] Tentando conectar ao data node em {ip}:{port}...")
                data_node_conn = rpyc.connect(ip, port)
                print(f"[STATUS] Conexão com o data node estabelecida em {ip}:{port}.")
                # TODO: tirar isso:
                data_node_conn.root.clear_storage_dir()
            except ConnectionRefusedError:
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                data_node_conn = None
                time.sleep(5)
        return data_node_conn
    

    def calculate_score(self, node_id):
        """Função para calcular o score com base nos recursos disponíveis"""
        # return 50
        data_node_conn = self.data_nodes[node_id]['conn']
        self.data_nodes[node_id]['status'] = data_node_conn.root.get_node_status()
        cpu, memory, disk = list(self.data_nodes[node_id]['status'].values())
        return (
            (100 - cpu)    * STATUS_WEIGHTS['cpu'] +
            (100 - memory) * STATUS_WEIGHTS['memory'] +
            (100 - disk)   * STATUS_WEIGHTS['disk']
        )


    def select_nodes_to_store(self):
        """
        Seleciona os data nodes para armazenamento de imagens
        Utiliza-se balanceamento de carga pelos recursos de máquina
        """
        scored_nodes = [
            (node_id, self.calculate_score(node_id))
            for node_id in self.data_nodes
        ]
        scored_nodes.sort(key=lambda x: x[1], reverse=True)
        storage_nodes = [node for node, score in scored_nodes if score >= BASE_SCORE]
        if len(storage_nodes) < self.replication_factor:
            storage_nodes = [node for node, _ in scored_nodes[:self.replication_factor]]
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
                    ((node_id, self.calculate_score(node_id)) for node_id in shard['nodes']),
                    key=lambda x: x[1],
                    reverse=True
                )
            ]
        print('retrieval_nodes =', retrieval_nodes)

        
        retrieval_nodes.append(scored_nodes[0])

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
        # img_01  ->  [[part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        for node_id in nodes_id:
            if node_id not in self.index_table[image_name][shard_index]:
                self.index_table[image_name][shard_index]['nodes'].append(node_id)
        self.index_table[image_name][shard_index]['size'] = shard_size
        # print('index_table:')
        # for k, v in self.index_table.items():
        #     print(f'{k}: {v}')
        # print()