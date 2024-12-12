import rpyc, math, threading, sys, time
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
CLUSTER_SIZE = 4 # Quantidade total de data nodes
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


    def exposed_init_upload_image_chunk(self, image_name, image_size): #try_exception
        try:
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
        except Exception as exception:
            self.cluster.rollback_update_index_table(self.current_image_name)
            raise exception


    def exposed_upload_image_chunk(self, image_chunk): #try_exception
        try:
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
        except Exception as exception:
            self.cluster.rollback_update_index_table(self.current_image_name)
            raise exception


    def update_index_table(self): # raise
        self.cluster.update_index_table(self.current_image_name, # img_01 
                                        self.current_shard_index, # part_0
                                        self.current_shard_accumulated_size, # 1.999 MB 
                                        self.selected_nodes) # [1 2]
        

    def exposed_init_download_image_chunk(self, image_name):  #try_exception
        try:
            if image_name not in self.cluster.index_table:
                return False, f'[ERRO] A imagem "{image_name}" não existe', None
            self.current_image_name = image_name
            self.selected_nodes = self.cluster.select_nodes_to_retrieve(image_name)
            self.current_shard_index = 0
            self.current_image_size = self.cluster.image_total_size(image_name)
            print(f'\n"{self.current_image_name}" - Parte {self.current_shard_index}')
            print(f'Selected node: {self.selected_nodes[self.current_shard_index]}')
            return True, None, self.current_image_size
        except Exception as exception:
            raise exception


    def exposed_download_image_chunk(self): #try_exception
        try:
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
        except Exception as exception:
            raise exception


    def fetch_image_chunk(self): # raise
        index = self.current_shard_index
        node_id = self.selected_nodes[index]
        node = self.cluster.data_nodes[node_id]
        image_chunk, eof = node['conn'].root.retrieve_image_chunk(self.current_image_name, index)
        return image_chunk, eof
        

    def exposed_list_images(self):  #try_exception
        """Lista todas as imagens que estão armazenadas"""
        try:
            # TODO: remover este trecho:
            print('-----------[DATA NODES INFO]-----------')
            for node_id, info in self.cluster.data_nodes.items():
                print(f'{node_id}: online={info['online']}, score={info['score']}')
            print('-------------[INDEX TABLE]-------------')
            for k, v in self.cluster.index_table.items():
                print(k, v)
            print('---------------------------------------')
            return list(self.cluster.index_table.keys())
        except Exception as exception:
            raise exception


    def exposed_delete_image(self, image_name):  #try_exception
        """Deleta uma imagem"""
        try:
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
        except Exception as exception:
            raise exception


    def register_name(self, name_service_conn, name): #raise
        if not name_service_conn.root.lookup(name):
            name_service_conn.root.register(name, self.host, self.port)
        print(f'[STATUS] Servidor registrado no serviço de nomes como "{name}".')
        return True


    def start(self): #try_exection
        try:
            server_thread = threading.Thread(target=self.start_server, daemon=True)
            cluster_sub_score_thread = threading.Thread(target=self.cluster.sub_score.start_listening_sub_score, daemon=True)
            cluster_sub_status_thread = threading.Thread(target=self.cluster.sub_status.start_listening_sub_status, daemon=True)
            
            # Inicia as threads
            cluster_sub_score_thread.start()
            cluster_sub_status_thread.start()
            server_thread.start()
            
            while server_thread.is_alive() or \
                cluster_sub_score_thread.is_alive() or \
                cluster_sub_status_thread.is_alive():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("[KEYBOARD_INTERRUPT] Encerrando as threads.")
        except Exception as exception:
            raise exception


    def start_server(self): #raise
        t = ThreadedServer(service=self, port=self.port,
                           protocol_config={'allow_public_attrs': True})
        print(f'[STATUS] Servidor iniciado na porta {self.port}.')
        t.start()


if __name__ == "__main__":
    try:
        name_service_conn = rpyc.connect(HOST_NAME_SERVICE, PORT_NAME_SERVICE)
        geoeye_cluster = Cluster(CLUSTER_SIZE, REPLICATION_FACTOR, name_service_conn)
        server = Server(host=HOST_SERVER, port=PORT_SERVER, cluster=geoeye_cluster)
        if server.register_name(name_service_conn, NAME_SERVER):
            server.start()
    except ConnectionRefusedError:
        print('[ERRO] Não foi possível estabelecer conexão com o serviço de nomes.')
        sys.exit(0)
    except Exception:
        print('[ERRO] Alguma falha ocorreu nos sistemas do servidor.')
