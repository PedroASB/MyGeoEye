import socket
import os
from serialization import *

HOST_NAME = socket.gethostname()
IP_DATA_NODE = socket.gethostbyname(HOST_NAME)
PORT_DATA_NODE = 8000

class DataNode:
    IMAGES_DIR = 'database'

    def __init__(self, ip=IP_DATA_NODE, port=PORT_DATA_NODE):
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.server_socket = None


    def store_image(self, connection, directory):
        """Armazena a imagem"""
        image_name = deserialize_string(connection)
        image_size = deserialize_int(connection)
        image_path = os.path.join(directory, image_name)

        print('image_name =', image_name)
        print('image_size =', image_size)
        print('image_path =', image_path)

        with open(image_path, 'wb') as file:
            received_size = 0
            while received_size < image_size:
                data = connection.recv(CHUNK_SIZE)
                file.write(data)
                received_size += len(data)
        print(f'Imagem "{image_name}" armazenada com sucesso.')

    def send_image(self, connection, directory):
        """Envia uma imagem para o cliente"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        print(f'Enviando imagem "{image_name}')
        if not os.path.exists(image_path):
            return
        image_size = os.path.getsize(image_path)
        serialize_int(connection, image_size)
        with open(image_path, 'rb') as file:
            while chunk := file.read(CHUNK_SIZE):
                connection.send(chunk)


    def delete_image(self, connection, directory):
        """Deleta uma imagem"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        print(f'Deletando imagem "{image_name}')
        # TODO: retornar false se algum erro ocorrer
        if not os.path.exists(image_path):
            return
        os.remove(image_path)


    def handle_server(self, connection, address):
        """Gerencia a conexão com um cliente"""
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
                    print(f'[ERRO] {address[0]}:{address[1]}: <comando inválido>')

        self.server_socket.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o data node"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.address)
        self.server_socket.listen(1)
        print(f'[STATUS] Data Node iniciado em {IP_DATA_NODE}:{PORT_DATA_NODE}.')
        while True:
            connection, address = self.server_socket.accept()
            self.handle_server(connection, address)


if __name__ == "__main__":
    data_node = DataNode()  # Instancia o objeto Cluster
    data_node.start()  # Inicia o loop de espera por conexões