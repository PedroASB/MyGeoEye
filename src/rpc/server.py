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
        print('[STATUS] Servidor inicializado com o cluster.')
        self.current_image_name = None
        self.current_image_size = None
        self.current_image_part = None
        self.current_image_size_division = None
        self.current_image_accumulated_size = None
        self.round_robin_nodes = None
        self.current_storage_nodes = None


    def on_connect(self, conn):
        print("[STATUS] Cliente conectado.")


    def on_disconnect(self, conn):
        print("[STATUS] Cliente desconectado.")

    def exposed_init_upload_image_chunk(self, image_name, image_size):
        self.current_image_name = image_name
        self.current_image_size = image_size
        self.current_image_size_division = math.ceil(image_size / SHARD_SIZE)
        self.current_image_part = 0
        self.current_image_accumulated_size = 0
        self.current_storage_nodes = {}
        
        if not self.cluster.init_update_index_table(self.current_image_name,
                                                    self.current_image_size_division):
            return False, "[ERRO] Nome de imagem já existente."

        # Seleciona os nós para o armazenamento
        self.round_robin_nodes = cycle(self.cluster.select_nodes_to_store())
        
        # Seleciona os primeiros nós para a parte inicial da imagem
        self.current_storage_nodes = list(islice(self.round_robin_nodes,
                                                 self.REPLICATION_FACTOR))
        
        return True, ""
        # print(f'image_size = {image_size}, division = {image_size / SHARD_SIZE}')
        # print(f'current_image_size_division = {self.current_image_size_division}')
        # print('current_storage_nodes:', end=' ')
        # for n in self.current_storage_nodes:
        #     print(n, end=' ')
        # print()

    def exposed_upload_image_chunk(self, image_chunk):
        
        self.current_image_accumulated_size += CHUNK_SIZE
        # Muda para a próxima parte se necessário
        if self.current_image_accumulated_size > SHARD_SIZE:
            self.current_image_accumulated_size = 0
            self.current_image_part += 1
            # Seleciona os próximos nós para a nova parte da imagem
            self.current_storage_nodes = list(islice(self.round_robin_nodes,
                                                     self.REPLICATION_FACTOR))
            # print('current_storage_nodes:', end=' ')
            # for n in self.current_storage_nodes:
            #     print(n, end=' ')
            # print()

        # Envia o chunk para os nós selecionados
        for node_id in self.current_storage_nodes:
            node = self.cluster.data_nodes[node_id]
            node['conn'].root.store_image_chunk(self.current_image_name,
                                                self.current_image_part, image_chunk)
            self.cluster.update_index_table(self.current_image_name, self.current_image_part, node_id)


    def retrieve_image_from_parts(self, image_name):
        """Recupera uma imagem dividida em partes a partir dos nós selecionados."""
        parts = self.cluster.index_table.get(image_name, [])
        image_data = b""

        for node_id, part_num in parts:
            part_image_name = f"{image_name}%part%{part_num}"
            part_data = self.cluster.data_nodes[node_id]['conn'].exposed_retrieve_image(part_image_name)
            for chunk in part_data:
                image_data += chunk

        print(f'[STATUS] Imagem "{image_name}" recuperada com sucesso.')
        return image_data
    
    def exposed_download_image(self, image_name):
        if image_name in self.cluster.index_table:
            has_image = True
        else:
            has_image = False
        
        if not has_image:
            return
        
        # Selecionado o data node para realizar o upload
        node_id = self.cluster.select_nodes_to_retrieve(image_name)
        data_node_conn = self.cluster.data_nodes[node_id][1]

        print('[INFO] Data node selecionado para download:', node_id)
        
        image_data = data_node_conn.root.retrieve_image(image_name)

        return image_data


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
        threaded_server = ThreadedServer(service=Server, port=PORT_SERVER, protocol_config={'allow_public_attrs': True})
        threaded_server.start()
        print("[STATUS] Servidor iniciado.")


if __name__ == "__main__":
    server = Server()
    server.start()
