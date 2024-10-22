import socket
import threading
import os
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
        

    def save_image(self, connection, directory):
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
        print(f'Imagem "{image_name}" armazenada com sucesso.')


    def list_images(self, connection, directory):
        """Lista todas as imagens que o cliente salvou"""
        images = os.listdir(directory)
        num_images = len(images)
        serialize_int(connection, num_images)
        if num_images > 0:
            serialize_string(connection, '\n'.join(images))


    def send_image(self, connection, directory):
        """Envia uma imagem para o cliente"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        if not os.path.exists(image_path):
            serialize_bool(connection, False)
            return
        serialize_bool(connection, True)
        image_size = os.path.getsize(image_path)
        serialize_int(connection, image_size)
        with open(image_path, 'rb') as file:
            while chunk := file.read(CHUNK_SIZE):
                connection.send(chunk)


    def delete_image(self, connection, directory):
        """Deleta uma imagem"""
        image_name = deserialize_string(connection)
        image_path = os.path.join(directory, image_name)
        if not os.path.exists(image_path):
            serialize_bool(connection, False)
            return
        serialize_bool(connection, True)
        os.remove(image_path)


    def handle_client(self, connection, address):
        """Gerencia a conexão com um cliente"""
        print(f'[NOVA CONEXÃO] Cliente {address[0]}:{address[1]} conectado.')
        while True:
            option = deserialize_int(connection)
            match option:
                case 1: # Inserir imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Inserir imagem.')
                    self.save_image(connection, self.IMAGES_DIR)
                    
                case 2: # Baixar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Baixar imagem.')
                    self.send_image(connection, self.IMAGES_DIR)

                case 3: # Listar imagens
                    print(f'[COMANDO] {address[0]}:{address[1]}: Listar imagens.')
                    self.list_images(connection, self.IMAGES_DIR)
                        
                case 4: # Deletar imagem
                    print(f'[COMANDO] {address[0]}:{address[1]}: Deletar imagem.')
                    self.delete_image(connection, self.IMAGES_DIR)

                case 0: # Encerrar conexão
                    print(f'[COMANDO] {address[0]}:{address[1]}: Encerrar conexão.')
                    break
                
                case _:
                    print(f'[ERRO] {address[0]}:{address[1]}: <comando inválido>')

        connection.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o servidor"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.address)
        server_socket.listen()
        print(f'[STATUS] Servidor iniciado em {IP_SERVER}:{PORT_SERVER}.')
        while True:
            connection, address = server_socket.accept()
            thread = threading.Thread(target=self.handle_client, args=(connection, address))
            thread.start()
            print(f'Conexões ativas: {threading.active_count() - 1}')


if __name__ == '__main__':
    server = Server()
    server.start()
