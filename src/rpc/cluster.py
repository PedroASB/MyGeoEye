import rpyc
import time

STATUS_WEIGHTS = {"cpu": 0.3, "memory": 0.2, "disk": 0.5}

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
            except ConnectionRefusedError:
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                data_node_conn = None
                time.sleep(5)
        return data_node_conn
    

    def calculate_score(self,node_id):
        """Função para calcular o score com base nos recursos disponíveis"""
        # data_node = ['addr', 'conn', 'status']
        data_node_conn = self.data_nodes[node_id]['conn']
        self.data_nodes[node_id]['status'] = data_node_conn.get_node_status()
        cpu, memory, disk = self.data_nodes[node_id]['status'].values()
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

        # Calcula a pontuação de cada node com base nos recursos
        scored_nodes = sorted(self.data_nodes, key=self.calculate_score, reverse=True)
        # Seleciona os top K nodes com maior pontuação, onde K é o fator de réplica
        top_nodes = scored_nodes[:(self.division_factor * self.replication_factor)]
        return top_nodes


    def select_nodes_to_retrieve(self, image_name):
        """
        Seleciona os data nodes para recuperar uma imagem
        Utiliza-se balanceamento de carga pelos recursos de máquina
        """
        top_nodes = []
        # img_01  ->  ['nodes': [part_0: [3 2 4], part_1: [2 8 7], part_2: [1 4 3]]]
        for data_nodes_block in self.index_table[image_name]:
            # [3 2 4] -> sorted: [4 2 3]
            scored_nodes = sorted(data_nodes_block, key=self.calculate_score, reverse=True)
            # Seleciona o melhor nó de cada parte da imagem
            top_nodes.append(scored_nodes[:1]) # 4
        # top_nodes = [4 8 3]
        
        return top_nodes


    def update_index_table(self, image_name, node_id, part_num):
        """Atualiza a tabela de índices para uma imagem dividida em partes."""
        if image_name not in self.index_table:
            # [[], [], []]
            self.index_table[image_name] = [[] for _ in range(self.division_factor)]
        self.index_table[image_name][part_num].append(node_id)


"""
    self.data_node_id = {f'data_node_{i+1}': [self.data_nodes[i], None] for i in range(self.cluster_size)}
    
    self.data_node_addresses
    
, 
    DATA_NODE_1_ADDR = ('192.168.56.1', 8001)
    pesos
    data_node_1 = [DATA_NODE_1_ADDR, conn, info]

    data_node_1 = {'addr': ('192.168.56.1', 8001), 'conn': ???}

    data_nodes['data_node_1'] = {'addr': ('192.168.56.1', 8001), 'conn': ???, {'cpu': 3.5, 'memory': 30.2, 'disk': 25.5}}

    data_nodes_status['data_node_1'] = {'cpu': 3.5, 'memory': 30.2, 'disk': 25.5}

"""

# MEMÓRIA RAM
# img_01  ->  ['nodes': [0: [2 3 4], 1: [3 7 8], 2: [2 3 4]]]
# img_02  ->  ['nodes': [4 5 6], [4 5 6], 'next_index': 1]
# img_03  ->  ['nodes': [1 2 3], 'next_index': 1]

# index_table[img_01] = [ [2 3 4], [3 7 8] ]
# 100 MB -> 5 * 20 MB | 1000 MB -> 50 * 20 MB
# DIVISION_FACTOR = 3
# 320 MB 
# 106,6 -> 107
# img1%part%0%
# img_01.jpg%part%001%
# img1%part%2%

# image_part_size = ceil(image_size // self.DIVISION_FACTOR)
# selected_data_nodes = ...
# 

# 0 - 107 MB (64kb+..+...)
# 107 - 214
# 214 - 320

# imagem: 1000 MB
# chunk: 50 MB

# 334 MB / 334 MB / 332 MB

# 50 - 100 - 150 - 200 - 250 - 300 [334] 350 <-
# 400 - 450 - 500 - 550 - 600 - 650 
# 700 - 750 - 

# 70 MB 50 MB 