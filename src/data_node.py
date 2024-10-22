import socket
import threading
import os
from serialization import *

HOST_NAME = socket.gethostname()
IP_CLUSTER = socket.gethostbyname(HOST_NAME)
PORT_CLUSTER = 6000

class DataNode:
    IMAGES_DIR = 'database'

    def __init__(self, ip=IP_CLUSTER, port=PORT_CLUSTER):
        # Se o diretório de imagens não existir, ele será criado
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)

        # Cria o socket do cluster (TCP/IP) e associa-o ao endereço e porta
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.server_socket = None


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


    def handle_server(self, connection, address):
        """Gerencia a conexão com um cliente"""
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

        self.server_socket.close()
        print(f'[CONEXÃO ENCERRADA] Cliente {address[0]}:{address[1]} desconectado.')


    def start(self):
        """Inicializa o data node"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.address)
        self.server_socket.listen(1)
        print(f'[STATUS] Data Node iniciado em {IP_CLUSTER}:{PORT_CLUSTER}.')
        while True:
            # TODO: confirmar connection e address
            connection, address = self.server_socket.accept()
            # self.handle_server(connection, address)
            thread = threading.Thread(target=self.handle_server, args=(connection, address))
            thread.start()
            print(f'Conexões ativas: {threading.active_count() - 1}')


if __name__ == "__main__":
    data_node = DataNode()  # Instancia o objeto Cluster
    data_node.start()  # Inicia o loop de espera por conexões