import sys
import rpyc
import math
import threading
from itertools import cycle, islice
from rpyc.utils.server import ThreadedServer
from cluster import *
from addresses import *


# Servidor
NAME_SERVER = 'geoeye_images'
HOST_SERVER = 'localhost'
PORT_SERVER = 5000

# Serviço de nomes
HOST_NAME_SERVICE = 'localhost'
PORT_NAME_SERVICE = 6000

# Tamanho de chunks / fragmentos
CHUNK_SIZE = 65_536     # 64 KB
SHARD_SIZE = 2_097_152  # 2 MB


DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
REPLICATION_FACTOR = 2 # Fator de réplica


# TODO: criar uma classe "buffer" para armazenar variáveis 'current'
class Server(rpyc.Service):
    def __init__(self, host, port, cluster):
        self.current_image_name = None
        self.current_image_size = None
        self.current_shard_index = None
        self.current_image_chunk = None
        self.current_image_size_division = None
        self.current_shard_accumulated_size = None
        self.current_image_accumulated_size = None
        self.round_robin_nodes = None
        self.selected_nodes = None

        self.host = host
        self.port = port
        self.cluster = cluster
        self.replication_factor = self.cluster.replication_factor
        self.cluster.connect_cluster()
        print('\n[STATUS] Servidor inicializado com o cluster.')


    def on_connect(self, conn):
        print("[STATUS] Cliente conectado.")

    def on_disconnect(self, conn):
        print("[STATUS] Cliente desconectado.")


    def exposed_init_upload_image_chunk(self, image_name, image_size):
        self.current_image_name = image_name
        self.current_image_size = image_size
        self.current_image_size_division = math.ceil(image_size / SHARD_SIZE)
        self.current_shard_index = 0
        self.current_shard_accumulated_size = 0
        self.current_image_accumulated_size = 0
        self.selected_nodes = {}
        
        if not self.cluster.init_update_index_table(self.current_image_name,
                                                    self.current_image_size_division):
            return False, "[ERRO] Nome de imagem já existente."

        # Seleciona os nós para o armazenamento
        self.round_robin_nodes = cycle(self.cluster.select_nodes_to_store())
        
        # Seleciona os primeiros nós para a parte inicial da imagem
        self.selected_nodes = list(islice(self.round_robin_nodes, self.replication_factor))
        
        return True, None
        # print(f'image_size = {image_size}, division = {image_size / SHARD_SIZE}')
        # print(f'current_image_size_division = {self.current_image_size_division}')
        # print('current_storage_nodes:', end=' ')
        # for n in self.current_storage_nodes:
        #     print(n, end=' ')
        # print()


    def update_index_table(self):
        self.cluster.update_index_table(self.current_image_name, # img_01 
                                        self.current_shard_index, # part_0
                                        self.current_shard_accumulated_size, # 1.999 MB 
                                        self.selected_nodes) # [1 2]


    def exposed_upload_image_chunk(self, image_chunk):
        image_chunk_size = len(image_chunk)
        if self.current_shard_accumulated_size + image_chunk_size > SHARD_SIZE:
            self.update_index_table()
            self.current_shard_accumulated_size = 0
            self.current_shard_index += 1
            self.selected_nodes = list(islice(self.round_robin_nodes, self.replication_factor))

        self.current_shard_accumulated_size += image_chunk_size
        self.current_image_accumulated_size += image_chunk_size

        if self.current_image_size == self.current_image_accumulated_size:
            self.update_index_table()

        # Envia o chunk para os nós selecionados
        for node_id in self.selected_nodes:
            node = self.cluster.data_nodes[node_id]
            node['conn'].root.store_image_chunk(self.current_image_name,
                                                self.current_shard_index, image_chunk)
 

    def exposed_init_download_image_chunk(self, image_name):
        if image_name not in self.cluster.index_table:
            return False, f'[ERRO] A imagem "{image_name}" não existe', None
        self.current_image_name = image_name
        self.selected_nodes = self.cluster.select_nodes_to_retrieve(image_name)
        self.current_shard_index = 0
        self.current_image_size = self.cluster.image_total_size(image_name)
        print(f'\n"{self.current_image_name}" - Parte {self.current_shard_index}')
        print(f'Selected node: {self.selected_nodes[self.current_shard_index]}')
        return True, None, self.current_image_size


    def exposed_download_image_chunk(self):
        if self.current_shard_index < len(self.selected_nodes):
            image_chunk, eof = self.fetch_image_chunk()
            if eof:
                self.current_shard_index += 1
                if self.current_shard_index < len(self.selected_nodes):
                    print(f'\n"{self.current_image_name}" - Parte {self.current_shard_index}')
                    print(f'Selected node: {self.selected_nodes[self.current_shard_index]}')
                    image_chunk, eof = self.fetch_image_chunk()
        else:
            return 'error'
            
        return image_chunk


    def fetch_image_chunk(self):
        index = self.current_shard_index
        node_id = self.selected_nodes[index]
        node = self.cluster.data_nodes[node_id]
        image_chunk, eof = node['conn'].root.retrieve_image_chunk(self.current_image_name, index)
        return image_chunk, eof
        

    def exposed_list_images(self):
        """Lista todas as imagens que estão armazenadas"""
        return list(self.cluster.index_table.keys())


    def exposed_delete_image(self, image_name):
        """Deleta uma imagem"""
        if image_name not in self.cluster.index_table:
            return False
        # img_01  ->  [[part_0: {'nodes': 3 2 4, 'size': 100}, ...]
        # self.index_table[image_name][shard_index]['nodes']
        print(f'[INFO] Deletando a imagem {image_name}')
        for shard_index, shard in enumerate(self.cluster.index_table[image_name]):
            print(f'Delentando fragmento #{shard_index} de ', end='')
            for node_id in shard['nodes']:
                print(f'{node_id}', end=' ')
                data_node_conn = self.cluster.data_nodes[node_id]['conn']
                data_node_conn.root.delete_image(image_name, shard_index)
            print()
        del self.cluster.index_table[image_name]
        return True


    def register_name(self, name, host_name_service, port_name_service):
        try:
            name_service_conn = rpyc.connect(host_name_service, port_name_service)
        except ConnectionRefusedError:
            print('[ERRO] Não foi possível estabelecer conexão com o serviço de nomes.')
            return False
        if not name_service_conn.root.lookup(name):
            name_service_conn.root.register(name, self.host, self.port)
        print(f'[STATUS] Servidor registrado no serviço de nomes como "{name}".')
        return True


    # def start(self):
    #     t = ThreadedServer(service=self, port=self.port,
    #                        protocol_config={'allow_public_attrs': True})
    #     # self.start_listening()
    #     thread = threading.Thread(target=self.cluster.start_listening)
    #     #thread_2 = threading.Thread(target=self.listen_queue_2)
    #     thread.start()
    #     thread.join()
    #     print(f'[STATUS] Servidor iniciado na porta {self.port}.')
    #     t.start()


    def start(self):
        server_thread = threading.Thread(target=self.start_server, daemon=True)
        cluster_sub_score_thread = threading.Thread(target=self.cluster.subScore.start_listening_sub_score, daemon=True)
        cluster_sub_status_thread = threading.Thread(target=self.cluster.subStatus.start_listening_sub_status, daemon=True)

        # Inicia as threads
        cluster_sub_score_thread.start()
        cluster_sub_status_thread.start()
        server_thread.start()

        try:
            while server_thread.is_alive() or \
                cluster_sub_score_thread.is_alive() or \
                cluster_sub_status_thread.is_alive():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("[KEYBOARD_INTERRUPT] Encerrando as threads.")

        # Aguarda as threads finalizarem
        # cluster_sub_score_thread.join()
        # cluster_sub_status_thread.join()
        # server_thread.join()




    def start_server(self):
        t = ThreadedServer(service=self, port=self.port,
                           protocol_config={'allow_public_attrs': True})
        print(f'[STATUS] Servidor iniciado na porta {self.port}.')
        t.start()


if __name__ == "__main__":
    geoeye_cluster = Cluster(DATA_NODES_ADDR, REPLICATION_FACTOR)
    server = Server(host=HOST_SERVER, port=PORT_SERVER, cluster=geoeye_cluster)
    if server.register_name(NAME_SERVER, HOST_NAME_SERVICE, PORT_NAME_SERVICE):
        server.start()
