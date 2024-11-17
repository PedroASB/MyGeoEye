import rpyc
from itertools import cycle, islice
from rpyc.utils.server import ThreadedServer
from cluster import *
from addresses import *


PORT_SERVER = 5000
CHUNK_SIZE = 65_536 # Tamanho de um chunk (64 KB)
SHARD_SIZE = 2_097_152 # Tamanho de cada fragmento de imagem (2 MB)

class Server(rpyc.Service):
    DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR]
    REPLICATION_FACTOR = 2 # Fator de réplica

    def __init__(self):
        self.cluster = Cluster(self.DATA_NODES_ADDR, self.REPLICATION_FACTOR, SHARD_SIZE)
        self.cluster.connect_cluster()
        print('[STATUS] Servidor inicializado com o cluster.')
        self.current_data_nodes = None
        self.current_image_name = None
        self.current_image_size = None
        self.current_image_part = None
        self.current_image_tamanho_acomulado = 0
        self.current_data_nodes_index = None
        self.round_robin_nodes = None


    def on_connect(self, conn):
        print("[STATUS] Cliente conectado.")


    def on_disconnect(self, conn):
        print("[STATUS] Cliente desconectado.")

    
    def exposed_init_upload_image_chunk(self, image_name, image_size):
        self.current_image_name = image_name
        self.current_image_size = image_size
        self.current_image_part = 0
        self.current_data_nodes_index = 0
        # self.current_data_nodes = self.cluster.select_nodes_to_store()
        # self.round_robin = cycle(self.current_data_nodes)
        self.round_robin_nodes = cycle(self.cluster.select_nodes_to_store())
        self.current_data_nodes = islice(self.round_robin_nodes, self.current_data_nodes_index, \
                                         self.current_data_nodes_index + self.REPLICATION_FACTOR)
                
    def exposed_upload_image_chunk(self, image_chunk):
        # ========================================================
        # [node_0, node_1, node_2, node_3]
        # [node_0, node_1]
        # [node_2, node_3]
        results = islice(self.round_robin_nodes, self.current_data_nodes_index, self.current_data_nodes_index + self.REPLICATION_FACTOR)
        for node in results:
            if self.current_image_tamanho_acomulado + image_chunk <= SHARD_SIZE:
                self.current_image_tamanho_acomulado += image_chunk
                ##
            else:
                self.current_image_tamanho_acomulado = image_chunk
                self.current_image_part += 1
                ##

            node['conn'].root.store_image_chunk(image_chunk, self.current_image_part)
        
        self.current_data_nodes_index += self.REPLICATION_FACTOR
            
    
    # def exposed_end_upload_image_chunk(self, image_name, image_chunk):
    #     for node_id in self.current_data_nodes:
    #         data_node_conn = self.cluster.data_nodes[node_id][1]
    #         # Recebe o chunk do cliente e o encaminha imediatamente ao data node
    #         data_node_conn.root.store_image_chunk(image_name, image_chunk)
    #         print(f"Chunk de {len(image_chunk)} bytes encaminhado ao Data Node para {image_name}")


    def exposed_receive_chunk(self, chunk):
        pass


    def exposed_upload_image(self, image_name, image_data):
        self.current_data_nodes = self.cluster.select_nodes_to_store()
        print('[INFO] Data nodes selecionados para armazenamento: ', end='')
        for node in self.current_data_nodes:
            print(node, end=' ')
        for node_id in self.current_data_nodes:
            self.cluster.update_index_table(image_name, node_id, part)
            self.node_id


    def store_image_in_parts(self, image_name, image_data):
        """Divide a imagem em N partes e armazena em K data nodes selecionados."""
        nodes_to_store = self.cluster.select_nodes_to_store()
        num_parts = len(nodes_to_store)
        part_size = len(image_data) // num_parts

        for i, node_id in enumerate(nodes_to_store):
            part_data = image_data[i * part_size : (i + 1) * part_size]
            part_image_name = f"{image_name}%part%{i+1}"
            self.cluster.data_nodes[node_id]['conn'].exposed_store_image(part_image_name, part_data)
            self.cluster.update_index_table(image_name, node_id, i+1)
        print(f'[STATUS] Imagem "{image_name}" dividida e armazenada com sucesso.')


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

    def start(self):
        threaded_server = ThreadedServer(service=Server, port=PORT_SERVER)
        threaded_server.start()
        print("[STATUS] Servidor iniciado.")


if __name__ == "__main__":
    server = Server()
    server.start()
