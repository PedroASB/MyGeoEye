import socket
import threading
import os
import time
from serialization import *

HOST_NAME = socket.gethostname()
IP_SERVER = socket.gethostbyname(HOST_NAME)
PORT_SERVER = 5000
IP_CLUSTER = socket.gethostbyname(HOST_NAME)
PORT_CLUSTER = 6000
ADDRESS = (IP_SERVER, PORT_SERVER)

class Server:
    IMAGES_DIR = 'images_database'

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER, cluster_ip=IP_CLUSTER, cluster_port=PORT_CLUSTER):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.cluster_ip = cluster_ip
        self.cluster_port = cluster_port
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)
        self.client_socket = None
        self.cluster_socket = None
        self.connect_cluster()


    def connect_cluster(self):
        """Tenta conectar ao cluster com reconexões automáticas."""
        while self.cluster_socket is None:
            try:
                print(f"[STATUS] Tentando conectar ao cluster em {self.cluster_ip}:{self.cluster_port}...")
                # Cria um socket e tenta conectar ao cluster
                self.cluster_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.cluster_socket.connect((self.cluster_ip, self.cluster_port))
                print(f"[STATUS] Conexão com o cluster estabelecida")
            except ConnectionRefusedError:
                # Caso a conexão falhe, espera 5 segundos e tenta novamente
                print(f"[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                self.cluster_socket = None
                time.sleep(5)


    def reconect_cluster(self):
        """Tenta reconectar ao cluster caso a conexão caia."""
        print("[STATUS] Tentando reconectar ao cluster...")
        self.disconnect_cluster()  # Desconecta o socket atual
        self.connect_cluster()  # Tenta se conectar novamente


    def disconnect_cluster(self):
        """Desconecta do cluster."""
        if self.cluster_socket:
            print("[STATUS] Desconectando do cluster")
            self.cluster_socket.close()  # Fecha a conexão
            self.cluster_socket = None  # Define como None para tentar reconectar

    def save_image(self, connection):
        """Armazena a imagem"""
        image_name = deserialize_string(connection)
        image_size = deserialize_int(connection)

        serialize_string(self.cluster_socket, image_name)
        serialize_int(self.cluster_socket, image_size)
        
        received_size = 0
        while received_size < image_size:
            chunk = connection.recv(CHUNK_SIZE)
            self.cluster_socket.send(chunk)
            received_size += len(chunk)

        print(f'\nImagem "{image_name}" enviada com sucesso.')


    def list_images(self, connection):
        """Lista todas as imagens que o cliente salvou"""
        num_images = deserialize_int(self.cluster_socket)
        serialize_int(connection, num_images)
        if num_images > 0:
            images_list = deserialize_string(self.cluster_socket)
            serialize_string(connection, images_list)


    def send_image(self, connection):
        """Envia uma imagem para o cliente"""
        image_name = deserialize_string(connection)
        serialize_string(self.cluster_socket, image_name)
        has_image = deserialize_bool(self.cluster_socket)
        serialize_bool(connection, has_image)
        
        if not has_image:
            return
        
        image_size = deserialize_int(self.cluster_socket)
        serialize_int(connection, image_size)

        received_size = 0
        while received_size < image_size:
            # TODO: criar função serialize/deserialze para chunks
            chunk = self.cluster_socket.recv(CHUNK_SIZE)
            connection.send(chunk)
            received_size += len(chunk)
                

    def delete_image(self, connection):
        """Deleta uma imagem"""
        image_name = deserialize_string(connection)
        serialize_string(self.cluster_socket, image_name)

        has_image = deserialize_bool(self.cluster_socket)
        serialize_bool(connection, has_image)


    def handle_client(self, connection, address):
        """Gerencia a conexão com um server"""
        print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
        while True:
            option = deserialize_int(connection)
            match option:
                case 1: # Inserir imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                    serialize_int(self.cluster_socket, 1)
                    self.save_image(connection)
                    
                case 2: # Baixar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                    serialize_int(self.cluster_socket, 2)
                    self.send_image(connection)

                case 3: # Listar imagens
                    print(f'[COMANDO] {address[0]}:{address[1]}: Listar imagens.')
                    serialize_int(self.cluster_socket, 3)
                    self.list_images(connection)
                        
                case 4: # Deletar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Deletar imagem.')
                    serialize_int(self.cluster_socket, 4)
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
