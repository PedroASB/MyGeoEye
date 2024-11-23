import rpyc
import math
from itertools import cycle, islice
from rpyc.utils.server import ThreadedServer
from cluster import *
from addresses import *


PORT_SERVER = 5000
CHUNK_SIZE = 65_536 # Tamanho de um chunk (64 KB)
SHARD_SIZE = 2_097_152 # Tamanho de cada fragmento de imagem (2 MB)

# Ideia: criar uma classe "buffer" para armazenar variáveis current

class Server(rpyc.Service):
    DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
    REPLICATION_FACTOR = 2 # Fator de réplica
    # DATA_NODES_ADDR = [DATA_NODE_1_ADDR]
    # REPLICATION_FACTOR = 1 # Fator de réplica


    def __init__(self):
        self.cluster = Cluster(self.DATA_NODES_ADDR, self.REPLICATION_FACTOR, SHARD_SIZE)
        self.cluster.connect_cluster()
        print('\n[STATUS] Servidor inicializado com o cluster.')
        self.current_image_name = None
        self.current_image_size = None
        self.current_shard_index = None
        self.current_image_chunk = None
        self.current_image_size_division = None
        self.current_shard_accumulated_size = None
        self.current_image_accumulated_size = None
        self.round_robin_nodes = None
        self.selected_nodes = None


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
        self.selected_nodes = list(islice(self.round_robin_nodes,
                                                 self.REPLICATION_FACTOR))
        
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
            self.selected_nodes = list(islice(self.round_robin_nodes,
                                                     self.REPLICATION_FACTOR))

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
        # for image_name in self.cluster.index_table:
        #     return image_name
        return list(self.cluster.index_table.keys())


    def exposed_delete_image(self, image_name):
        """Deleta uma imagem"""
        if image_name in self.cluster.index_table:
            has_image = True
            # img_01  ->  [[3 4], 1]
            print(f'[INFO] Deletando a imagem {image_name} de: ', end='')
            for node_id in self.cluster.index_table[image_name][0]:
                print(f'{node_id}', end=' ')
                data_node_conn = self.cluster.data_nodes[node_id][1] # 3 4
                data_node_conn.root.delete_image(image_name)
            del self.cluster.index_table[image_name]
        else:
            has_image = False

        return has_image


    def start(self):
        threaded_server = ThreadedServer(service=Server, port=PORT_SERVER,
                                         protocol_config={'allow_public_attrs': True})
        threaded_server.start()
        print("[STATUS] Servidor iniciado.")


if __name__ == "__main__":
    server = Server()
    server.start()
