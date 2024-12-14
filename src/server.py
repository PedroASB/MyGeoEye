import rpyc, math, threading, sys, time, os
from itertools import cycle, islice
from rpyc.utils.server import ThreadedServer
from cluster import *
import concurrent.futures
# from addresses import *


# Servidor
NAME_SERVER = 'geoeye_images'
HOST_SERVER = '192.168.40.117' # 'localhost'
PORT_SERVER = 5000

# Serviço de nomes
HOST_NAME_SERVICE = '192.168.40.223' # 'localhost'
PORT_NAME_SERVICE = 6000

# Tamanho de chunks / fragmentos
CHUNK_SIZE = 65_536     # 64 KB
SHARD_SIZE = 2_097_152  # 2 MB


# DATA_NODES_ADDR = [DATA_NODE_1_ADDR, DATA_NODE_2_ADDR, DATA_NODE_3_ADDR, DATA_NODE_4_ADDR]
CLUSTER_SIZE = 1 # Quantidade total de data nodes
REPLICATION_FACTOR = 1 # Fator de réplica


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
        self.current_complete_image = None
        self.round_robin_nodes = None
        self.selected_nodes = None
        self.open_files = {}

        self.host = host
        self.port = port
        self.cluster = cluster
        self.lock = cluster.lock
        self.replication_factor = self.cluster.replication_factor
        assert self.cluster.cluster_size >= self.replication_factor,\
                "'cluster_size' deve ser maior ou igual do que 'replication_factor'"
        self.cluster.connect_cluster()
        print('\n[STATUS] Servidor inicializado com o cluster.')


    def on_connect(self, conn):
        client_host = conn._channel.stream.sock.getpeername()[0]
        print(f"[STATUS] Cliente conectado: '{client_host}'")


    def on_disconnect(self, conn):
        client_host = conn._channel.stream.sock.getpeername()[0]
        print(f"[STATUS] Cliente desconectado: '{client_host}'")


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
            storage_nodes = self.cluster.select_nodes_to_store()
            self.round_robin_nodes = cycle(storage_nodes)

            # Seleciona os primeiros nós para a parte inicial da imagem
            self.selected_nodes = list(islice(self.round_robin_nodes, self.replication_factor))
            print(f'[INFO] "{self.current_image_name}" / fragmento {self.current_shard_index} / armazenando em {self.selected_nodes}')
            
            return True, None
        except Exception as exception:
            print('[ERRO] Erro em exposed_init_upload_image_chunk')
            print('exception =', exception)
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
                print(f'[INFO] "{self.current_image_name}" / fragmento {self.current_shard_index} / armazenando em {self.selected_nodes}')

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
            print('[ERRO] Erro em exposed_upload_image_chunk')
            print('exception =', exception)
            self.cluster.rollback_update_index_table(self.current_image_name)
            raise exception


    def update_index_table(self): # raise
        # print('[INFO] Acessando o cluster para atualizar a tabela de índices.')
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
            start_time = time.time()
            self.download_image_complete()
            end_time = time.time()
            print(f"[INFO] Tempo de construção da imagem no servidor na RAM: {(end_time - start_time)} s")
            return True, None, self.current_image_size
        except Exception as exception:
            raise exception


    def exposed_download_image_chunk_old(self): #try_exception
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
        
    
    def exposed_download_image_chunk(self): #try_exception
        try:
            # image_shard_name = f'{image_name}%part{shard_index}%'
            # image_path = os.path.join(self.STORAGE_DIR, image_shard_name)
            image_path = self.current_image_name
            
            # Verificar se o arquivo já está aberto ou não
            if self.current_image_name not in self.open_files:
                if not os.path.exists(image_path):
                    print(f"[ERRO] {self.current_image_name} não encontrado.")
                    return None
                self.open_files[self.current_image_name] = open(image_path, "rb")

            file = self.open_files[self.current_image_name]
            image_chunk = file.read(CHUNK_SIZE)
            
            # Significa que temos o último "chunk" ou vazio
            if len(image_chunk) < CHUNK_SIZE:
                file.close()
                del self.open_files[self.current_image_name]
            
            return image_chunk
        except Exception as exception:
            print('[ERRO] Erro em exposed_retrieve_image_chunk')
            print('exception =', exception)
            try:
                file.close()
            except Exception:
                pass
            if self.current_image_name in self.open_files:
                del self.open_files[self.current_image_name]
            raise exception


    def fetch_image_chunk(self): # raise
        index = self.current_shard_index
        node_id = self.selected_nodes[index]
        node = self.cluster.data_nodes[node_id]
        image_chunk, eof = node['conn'].root.retrieve_image_chunk(self.current_image_name, index)
        return image_chunk, eof
    

    def download_image_complete(self):  #try_exception
        try:
            # Verifica se a imagem existe
            if self.current_image_name not in self.cluster.index_table:
                return False, "[ERRO] Imagem não encontrada."

            # Lista de shards para serem baixados
            shard_indices = range(len(self.selected_nodes))
            
            # Função auxiliar para baixar um shard
            def download_shard(index):
                node_id = self.selected_nodes[index]
                node = self.cluster.data_nodes[node_id]
                image_shard_name = f'{self.current_image_name}%part{index}%'
                image_shard = node['conn'].root.retrieve_image_shard(image_shard_name)
                return index, image_shard  # Retorna o índice e o shard baixado
            
            # Armazena os shards baixados
            downloaded_shards = {}

            # Cria um pool de threads para download paralelo
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_shard = {executor.submit(download_shard, index): index for index in shard_indices}
                for future in concurrent.futures.as_completed(future_to_shard):
                    index, shard_data = future.result()
                    downloaded_shards[index] = shard_data
            
            # Ordena os shards pela ordem do índice
            ordered_shards = [downloaded_shards[i] for i in sorted(downloaded_shards.keys())]
            
            # Concatena os shards em uma única sequência de bytes
            self.current_complete_image = b"".join(ordered_shards)

            image_path = self.current_image_name
            with open(image_path, "wb") as file:
                file.write(self.current_complete_image)
            
            # return self.current_complete_image
        except Exception as exception:
            print('[ERRO] Erro em exposed_download_image_complete.')
            print('exception =', exception)
            raise exception

    

    def exposed_list_images(self):  #try_exception
        """Lista todas as imagens que estão armazenadas"""
        try:
            return list(self.cluster.index_table.keys())
        except Exception as exception:
            raise exception


    def exposed_debug_info(self):  #try_exception
        """Lista todas as imagens que estão armazenadas"""
        try:
            # TODO: remover este trecho:
            print('-----------[DATA NODES INFO]-----------')
            for node_id, info in self.cluster.data_nodes.items():
                print(f"{node_id}: online={info['online']}, score={info['score']}")
            print('-------------[INDEX TABLE]-------------')
            for k, v in self.cluster.index_table.items():
                print(k, v)
            print('---------------------------------------')
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
    # except Exception:
        # print('[ERRO] Alguma falha ocorreu nos sistemas do servidor.')
