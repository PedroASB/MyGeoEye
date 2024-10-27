import socket
import os
from serialization import *

HOST_NAME = socket.gethostname()
IP_SERVER = socket.gethostbyname(HOST_NAME)
PORT_SERVER = 5000
ADDRESS_SERVER = (IP_SERVER, PORT_SERVER)

class Client:
    IMAGES_DIR = 'client_images'

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.client_socket = None
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)


    def upload_image(self, directory, image_name):
        """Envia uma imagem para o servidor"""
        serialize_string(self.client_socket, image_name)
        image_path = os.path.join(directory, image_name)
        image_size = os.path.getsize(image_path)
        serialize_int(self.client_socket, image_size)
        with open(image_path, 'rb') as file:
            while chunk := file.read(CHUNK_SIZE):
                self.client_socket.send(chunk)
        print(f'\nImagem "{image_name}" enviada com sucesso.')


    def download_image(self, directory, image_name):
        """Baixa uma imagem do servidor"""
        serialize_string(self.client_socket, image_name)
        has_image = deserialize_bool(self.client_socket)
        if not has_image:
            print('\n[ERRO] Arquivo de imagem não encontrado.')
            return
        image_size = deserialize_int(self.client_socket)
        image_path = os.path.join(directory, image_name)
        with open(image_path, 'wb') as file:
            received_size = 0
            while received_size < image_size:
                data = self.client_socket.recv(CHUNK_SIZE)
                file.write(data)
                received_size += len(data)
        print(f'Imagem "{image_name}" armazenada com sucesso em "{directory}/".')


    def list_images(self):
        """Lista as imagens que estão armazenadas no servidor"""
        num_images = deserialize_int(self.client_socket)
        if num_images > 0:
            print('\nImagens armazenadas:\n')
            for _ in range(num_images):
                image_name = deserialize_string(self.client_socket)
                print(image_name)
        else:
            print('\nNenhuma imagem armazenada.')


    def delete_image(self, image_name):
        """Deleta uma imagem"""
        serialize_string(self.client_socket, image_name)
        has_image = deserialize_bool(self.client_socket)
        if has_image:
            print(f'\nImagem "{image_name}" deletada com sucesso.')
        else:
            print('\n[ERRO] Arquivo de imagem não encontrado.')


    def start(self):
        """Inicializa o cliente"""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect(self.address)
        except Exception:
            print('[ERRO] Não foi possível estabelecer conexão com o servidor.')
            exit(0)
        print('[STATUS] Conexão com o servidor estabelecida.')
        
        while True:
            print('\nSelecione um comando:')
            print('1 - Enviar imagem')
            print('2 - Baixar imagem')
            print('3 - Listar imagens')
            print('4 - Deletar imagem')
            print('0 - Finalizar')
            command = input('>> ')
            match command:
                case '1': # Enviar imagem
                    image_name = str(input('\nImagem a ser enviada: '))
                    image_path = os.path.join(self.IMAGES_DIR, image_name)
                    if not os.path.exists(image_path):
                        print(f'\n[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
                        continue
                    serialize_int(self.client_socket, 1)
                    self.upload_image(self.IMAGES_DIR, image_name)

                case '2': # Baixar imagem
                    number_of_images = int(input('\nQuantidade de imagens a serem baixadas: '))
                    while number_of_images < 1:
                        number_of_images = int(input('Informe um valor maior ou igual a 1: '))
                    images = []
                    for _ in range(number_of_images):
                        images.append(str(input('Nome da imagem: ')))
                    for image_name in images:
                        serialize_int(self.client_socket, 2)
                        self.download_image(self.IMAGES_DIR, image_name)

                case '3': # Listar imagens
                    serialize_int(self.client_socket, 3)
                    self.list_images()

                case '4': # Deletar imagem
                    serialize_int(self.client_socket, 4)
                    image_name = str(input('\nImagem a ser deletada: '))
                    self.delete_image(image_name)

                case '0': # Encerrar conexão
                    serialize_int(self.client_socket, 0)
                    break

                case _:
                    print('\n[ERRO] Comando inválido.')
        
        self.client_socket.close()
        print('\n[STATUS] Conexão com o servidor encerrada.')


if __name__ == '__main__':
    client = Client()
    client.start()
