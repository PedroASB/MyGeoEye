import socket
import threading
from serialization import *
from addresses import *
from cluster import *

HOST_NAME = socket.gethostname()
IP_SERVER = socket.gethostbyname(HOST_NAME)
PORT_SERVER = 5000
ADDRESS_SERVER = (IP_SERVER, PORT_SERVER)


class Server:
    DATA_NODES = [DATA_NODE_1, DATA_NODE_2, DATA_NODE_3]
    REPLICATION_FACTOR = 2 # Fator de réplica

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.server_socket = None
        self.cluster = Cluster(self.DATA_NODES, self.REPLICATION_FACTOR)
        self.cluster.connect_cluster()


    def store_image(self, connection):
        """Armazena a imagem nos data nodes selecionados"""
        image_name = deserialize_string(connection)
        image_size = deserialize_int(connection)

        # MEMÓRIA RAM
        # img_01  ->  [[3 4], 1]
        selected_nodes = self.cluster.select_nodes_to_store()

        print('[INFO] Data nodes selecionados para armazenamento: ', end='')
        for node in selected_nodes:
            print(node, end=' ')

        for node_id in selected_nodes:
            data_node_socket = self.cluster.data_node_id[node_id][1]
            self.cluster.update_index_table(image_name, node_id)
            serialize_int(data_node_socket, 1)
            serialize_string(data_node_socket, image_name)
            serialize_int(data_node_socket, image_size)
        
        print('\n[INFO] Tabela de índices:')
        for key, value in self.cluster.index_table.items():
            print(f'{key}: {value[0]}')

        # Função para enviar um chunk para todos os data nodes simultaneamente
        def send_chunk_to_all_nodes(chunk):
            for node_id in selected_nodes:
                data_node_socket = self.cluster.data_node_id[node_id][1]
                try:
                    data_node_socket.sendall(chunk)
                except Exception as e:
                    print(f"[ERRO] Falha no envio para o nó {node_id}: {e}")

        # Recebe a imagem do cliente em chunks e envia cada chunk para os data nodes
        received_size = 0
        while received_size < image_size:
            chunk = connection.recv(CHUNK_SIZE)
            if not chunk:
                break
            # Envia o chunk para todos os nós selecionados
            send_chunk_to_all_nodes(chunk)
            received_size += len(chunk)

        print(f'\n[INFO] Imagem "{image_name}" armazenada com sucesso.')


    def list_images(self, connection):
        """Lista todas as imagens que estão armazenadas"""
        num_images = len(self.cluster.index_table)
        serialize_int(connection, num_images)
        for image_name in self.cluster.index_table:
            serialize_string(connection, image_name)


    def send_image(self, connection):
        """Envia uma imagem para o cliente"""
        # Recebendo o nome da imagem do cliente
        image_name = deserialize_string(connection)

        # Verifica se existe a imagem
        if image_name in self.cluster.index_table:
            has_image = True
        else:
            has_image = False
        # Envia se há uma imagem ou não para o cliente
        serialize_bool(connection, has_image)
        if not has_image:
            return
        
        # Selecionado o data node para realizar o upload
        node_id = self.cluster.select_node_to_retrieve(image_name)
        data_node_socket = self.cluster.data_node_id[node_id][1]

        print('[INFO] Data node selecionado para download:', node_id)
        
        serialize_int(data_node_socket, 2)
        serialize_string(data_node_socket, image_name)
        
        image_size = deserialize_int(data_node_socket)
        serialize_int(connection, image_size)

        received_size = 0
        while received_size < image_size:
            chunk = data_node_socket.recv(CHUNK_SIZE)
            connection.send(chunk)
            received_size += len(chunk)
                

    def delete_image(self, connection):
        """Deleta uma imagem"""
        image_name = deserialize_string(connection)
        if image_name in self.cluster.index_table:
            has_image = True
            # img_01  ->  [[3 4], 1]
            for node_id in self.cluster.index_table[image_name][0]:
                data_node_socket = self.cluster.data_node_id[node_id][1] # 3 4
                serialize_int(data_node_socket, 4)
                serialize_string(data_node_socket, image_name)
            del self.cluster.index_table[image_name]
        else:
            has_image = False
        serialize_bool(connection, has_image)


    def handle_client(self, connection, address):
        """Gerencia a conexão com o cliente"""
        print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
        while True:
            option = deserialize_int(connection)
            match option:
                case 1: # Inserir imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                    self.store_image(connection)
                    
                case 2: # Baixar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                    self.send_image(connection)

                case 3: # Listar imagens
                    print(f'[COMANDO] {address[0]}:{address[1]}: Listar imagens.')
                    self.list_images(connection)
                        
                case 4: # Deletar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Deletar imagem.')
                    self.delete_image(connection)

                case 0: # Encerrar conexão
                    print(f'[COMANDO] {address[0]}:{address[1]}: Encerrar conexão.')
                    break
                
                case _:
                    print(f'[ERRO] {address[0]}:{address[1]}: Comando inválido.')

        connection.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o servidor"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.address)
        self.server_socket.listen()
        print(f'[STATUS] Servidor iniciado em {IP_SERVER}:{PORT_SERVER}.')
        while True:
            connection, address = self.server_socket.accept()
            thread = threading.Thread(target=self.handle_client, args=(connection, address))
            thread.start()
            print(f'[STATUS] Conexões ativas: {threading.active_count() - 1}')


if __name__ == '__main__':
    server = Server()
    server.start()
