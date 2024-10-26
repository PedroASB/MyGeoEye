import socket
import threading
import time
from serialization import *
from addresses import *

HOST_NAME = socket.gethostname()
IP_SERVER = socket.gethostbyname(HOST_NAME)
PORT_SERVER = 5000
IP_CLUSTER = socket.gethostbyname(HOST_NAME)
PORT_CLUSTER = 6000
ADDRESS = (IP_SERVER, PORT_SERVER)

class Server:
    CLUSTER = [DATA_NODE_1, DATA_NODE_2, DATA_NODE_3]
    CLUSTER_SIZE = len(CLUSTER) # Total de data nodes
    REPLICATION_FACTOR = 2 # Fator de réplica
    # DATA_NODE_ID = {f'data_node_{i+1}': CLUSTER[i] for i in range(CLUSTER_SIZE)}

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        # self.cluster_ip = cluster_ip
        # self.cluster_port = cluster_port
        self.client_socket = None
        self.data_node_id = {f'data_node_{i+1}': [self.CLUSTER[i], None] for i in range(self.CLUSTER_SIZE)}
        self.connect_cluster()
        # self.data_node_socket = None # remover
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
        for _ in range(self.REPLICATION_FACTOR):
            selected_nodes.append(f'data_node_{self.current_node_insert}')
            self.current_node_insert += 1
            if self.current_node_insert > self.CLUSTER_SIZE:
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
        if self.index_img_table[image_name][1] > self.REPLICATION_FACTOR:
            self.index_img_table[image_name][1] = 1
        
        return selected_node_id


    def update_index_img_table(self, image_name, node_id):
        # img_01  ->  None
        if image_name not in self.index_img_table:
            # img_01  ->  [[], 1]
            self.index_img_table[image_name] = [[], 1]
        self.index_img_table[image_name][0].append(node_id)
    

    def store_image(self, connection):
        """Armazena a imagem"""
        image_name = deserialize_string(connection)
        image_size = deserialize_int(connection)

        # MEMÓRIA RAM
        # img_01  ->  [[3 4], 1]
        selected_nodes = self.select_data_nodes_insert()

        print('Selected data nodes:')
        print(selected_nodes)

        for node_id in selected_nodes:
            data_node_socket = self.data_node_id[node_id][1]
            self.update_index_img_table(image_name, node_id)
            serialize_int(data_node_socket, 1) # TODO: trocar por CMD_STORE = 1
            serialize_string(data_node_socket, image_name)
            serialize_int(data_node_socket, image_size)
        
        print('self.index_img_table:', self.index_img_table)
        print('==============')

        # Função para enviar um chunk para todos os data nodes simultaneamente
        # TODO: o que fazer se a imagem já existe no diretório?
        def send_chunk_to_all_nodes(chunk):
            for node_id in selected_nodes:
                data_node_socket = self.data_node_id[node_id][1]
                try:
                    data_node_socket.sendall(chunk)  # Envia o chunk para o data node
                except Exception as e:
                    print(f"Erro ao enviar para o nó {node_id}: {e}")

        # Recebe a imagem do cliente em chunks e envia para os data nodes
        received_size = 0
        while received_size < image_size:
            chunk = connection.recv(CHUNK_SIZE)
            if not chunk:
                break
            # Envia o chunk para todos os nós selecionados
            send_chunk_to_all_nodes(chunk)
            received_size += len(chunk)

        print(f'\nImagem "{image_name}" armazenada com sucesso.')


    def list_images(self, connection):
        """Lista todas as imagens que o cliente salvou"""
        num_images = len(self.index_img_table)
        serialize_int(connection, num_images)
        for image_name in self.index_img_table:
            serialize_string(connection, image_name)


    def send_image(self, connection):
        """Envia uma imagem para o cliente"""
        # Recebendo o nome da imagem do cliente
        image_name = deserialize_string(connection)

        # Verifica se existe a imagem
        if image_name in self.index_img_table:
            has_image = True
        else:
            has_image = False
        # Envia se há uma imagem ou não para o cliente
        serialize_bool(connection, has_image)
        if not has_image:
            return
        
        # Selecionado o data node para realizar o upload
        node_id = self.select_data_node_recover(image_name)
        data_node_socket = self.data_node_id[node_id][1]

        print('Data node selecionado p/ download:', node_id)
        
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

        # has_image = deserialize_bool(data_node_socket)
        if image_name in self.index_img_table:
            has_image = True
            # img_01  ->  [[3 4], 1]
            for node_id in self.index_img_table[image_name][0]:
                data_node_socket = self.data_node_id[node_id][1] # 3 4
                serialize_int(data_node_socket, 4)
                serialize_string(data_node_socket, image_name)
            del self.index_img_table[image_name]
        else:
            has_image = False

        serialize_bool(connection, has_image)


    # def reconect_data_node(self, address):
    #     """Tenta reconectar ao cluster caso a conexão caia."""
    #     print("[STATUS] Tentando reconectar ao cluster...")
    #     self.disconnect_data_node(address)  # Desconecta o socket atual
    #     self.connect_data_node(address)  # Tenta se conectar novamente


    # def disconnect_data_node(self, address):
    #     """Desconecta do cluster."""
    #     data_node_socket = data_node_id[key]
    #     if data_node_socket:
    #         print("[STATUS] Desconectando do cluster")
    #         data_node_socket.close()  # Fecha a conexão
    #         data_node_socket = None  # Define como None para tentar reconectar


    def handle_client(self, connection, address):
        """Gerencia a conexão com um server"""
        print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
        while True:
            option = deserialize_int(connection)
            match option:
                case 1: # Inserir imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                    self.store_image(connection) # save_image(conn, socket)
                    
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
                    print(f'[ERRO] {address[0]}:{address[1]}: <comando inválido>')

        connection.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o servidor"""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(self.address)
        self.client_socket.listen()
        print(f'[STATUS] Servidor iniciado em {IP_SERVER}:{PORT_SERVER}.')
        while True:
            connection, address = self.client_socket.accept()
            thread = threading.Thread(target=self.handle_client, args=(connection, address))
            thread.start()
            print(f'Conexões ativas: {threading.active_count() - 1}')


if __name__ == '__main__':
    server = Server()
    server.start()
