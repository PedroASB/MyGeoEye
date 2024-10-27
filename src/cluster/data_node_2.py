import socket
import os
from serialization import *

HOST_NAME = socket.gethostname()
IP_DATA_NODE = socket.gethostbyname(HOST_NAME)
PORT_DATA_NODE = 8002

class DataNode:
    IMAGES_DIR = 'database2'

    def __init__(self, ip=IP_DATA_NODE, port=PORT_DATA_NODE):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.data_node_socket = None
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)


    def store_image(self, connection, directory):
        """Armazena a imagem"""
        image_name = deserialize_string(connection)
        image_size = deserialize_int(connection)
        image_path = os.path.join(directory, image_name)
        with open(image_path, 'wb') as file:
            received_size = 0
            while received_size < image_size:
                data = connection.recv(CHUNK_SIZE)
                file.write(data)
                received_size += len(data)
        print(f'[INFO] Imagem "{image_name}" armazenada com sucesso.')


    def send_image(self, connection, directory):
        """Envia uma imagem para o cliente"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        if not os.path.exists(image_path):
            return
        image_size = os.path.getsize(image_path)
        serialize_int(connection, image_size)
        with open(image_path, 'rb') as file:
            while chunk := file.read(CHUNK_SIZE):
                connection.send(chunk)
        print(f'[INFO] Imagem "{image_name}" enviada com sucesso.')


    def delete_image(self, connection, directory):
        """Deleta uma imagem"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        if not os.path.exists(image_path):
            return
        os.remove(image_path)
        print(f'[INFO] Imagem "{image_name}" deletada com sucesso.')


    def handle_server(self, connection, address):
        """Gerencia a conexão com o servidor"""
        while True:
            option = deserialize_int(connection)
            match option:
                case 1: # Inserir imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                    self.store_image(connection, self.IMAGES_DIR)
                    
                case 2: # Baixar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                    self.send_image(connection, self.IMAGES_DIR)

                case 4: # Deletar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Deletar imagem.')
                    self.delete_image(connection, self.IMAGES_DIR)

                case 0: # Encerrar conexão
                    print(f'[COMANDO] {address[0]}:{address[1]}: Encerrar conexão.')
                    break
                
                case _:
                    print(f'[ERRO] {address[0]}:{address[1]}: Comando inválido.')

        self.data_node_socket.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o data node"""
        self.data_node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_node_socket.bind(self.address)
        self.data_node_socket.listen(1)
        print(f'[STATUS] Data Node iniciado em {IP_DATA_NODE}:{PORT_DATA_NODE}.')
        while True:
            connection, address = self.data_node_socket.accept()
            self.handle_server(connection, address)


if __name__ == "__main__":
    data_node = DataNode()
    data_node.start()