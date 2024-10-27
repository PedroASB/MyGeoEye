import socket
import time

class Cluster:
    def __init__(self, data_nodes, replication_factor):
        self.data_nodes = data_nodes
        self.cluster_size = len(data_nodes)
        self.replication_factor = replication_factor

        self.data_node_id = {f'data_node_{i+1}': [self.data_nodes[i], None] for i in range(self.cluster_size)}
        self.current_node_insert = 1
        self.current_node_recover = 1
        self.index_img_table = {}


    def connect_cluster(self):
        for key, value in self.data_node_id.items():
            # key: data_node_i
            # value: ((IP, PORT), socket)
            # self.data_node_id['data_node_3'][1] = self.connect_data_node(('1.1.1.1', '8003'))
            self.data_node_id[key][1] = self.connect_data_node(value[0])
        print('[STATUS] Todos os data nodes do cluster foram conectados com sucesso.')


    def connect_data_node(self, address):
        """Tenta conectar ao cluster com reconexões automáticas."""
        data_node_socket = None
        ip, port = address[0], address[1]
        while data_node_socket is None:
            try:
                print(f"[STATUS] Tentando conectar ao data node em {ip}:{port}...")
                # Cria um socket e tenta conectar ao data node
                data_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                data_node_socket.connect(address)
                print(f"[STATUS] Conexão com o data node estabelecida em {ip}:{port}")
                # self.data_nodes_sockets = data_node_socket
            except ConnectionRefusedError:
                # Caso a conexão falhe, espera 5 segundos e tenta novamente
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                data_node_socket = None
                time.sleep(5)
        return data_node_socket


    def select_data_nodes_insert(self):
        selected_nodes = []
        for _ in range(self.replication_factor):
            selected_nodes.append(f'data_node_{self.current_node_insert}')
            self.current_node_insert += 1
            if self.current_node_insert > self.cluster_size:
                self.current_node_insert = 1
        return selected_nodes


    def select_data_node_recover(self, image_name):
        # MEMÓRIA RAM
        # img_01  ->  [[2 3 4], 2]
        # img_02  ->  [[4 5 6], 1]
        # img_03  ->  [[1 2 3], 1]

        # image_name: img_01  ->  [[5 6 7 8], 1]
        index = self.index_img_table[image_name][1] - 1
        selected_node_id = self.index_img_table[image_name][0][index]

        self.index_img_table[image_name][1] += 1
        if self.index_img_table[image_name][1] > self.replication_factor:
            self.index_img_table[image_name][1] = 1
        
        return selected_node_id


    def update_index_img_table(self, image_name, node_id):
        # img_01  ->  None
        if image_name not in self.index_img_table:
            # img_01  ->  [[], 1]
            self.index_img_table[image_name] = [[], 1]
        self.index_img_table[image_name][0].append(node_id)
