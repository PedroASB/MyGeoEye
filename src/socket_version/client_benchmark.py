import socket
import os
from serialization import *
import threading
from time import time, sleep

HOST_NAME = socket.gethostname()
SERVER_IP = socket.gethostbyname(HOST_NAME)
PORT = 5000
ADDRESS = (SERVER_IP, PORT)

class Client:
    IMAGES_DIR = 'client_images'

    def __init__(self, ip=SERVER_IP, port=PORT):
        self.ip = ip
        self.port = port
        self.address = (self.ip, self.port)
        self.server_socket = None
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)

    def upload_image(self, directory, image_name):
        """Envia uma imagem para o servidor"""
        serialize_string(self.server_socket, image_name)
        image_path = os.path.join(directory, image_name)
        image_size = os.path.getsize(image_path)
        serialize_int(self.server_socket, image_size)
        with open(image_path, 'rb') as file:
            while chunk := file.read(CHUNK_SIZE):
                self.server_socket.send(chunk)
        # print(f'Imagem "{image_name}" enviada com sucesso.')

    def download_image(self, directory, image_name):
        """Baixa uma imagem do servidor"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.server_socket.connect(self.address)
        except Exception:
            print('[ERRO] Não foi possível estabelecer conexão com o servidor.')
            exit(0)
        serialize_int(self.server_socket, 2)
        serialize_string(self.server_socket, image_name)
        has_image = deserialize_bool(self.server_socket)
        if not has_image:
            print(f'\n[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
            return
        image_size = deserialize_int(self.server_socket)
        image_path = os.path.join(directory, image_name)
        with open(image_path, 'wb') as file:
            received_size = 0
            while received_size < image_size:
                data = self.server_socket.recv(CHUNK_SIZE)
                file.write(data)
                received_size += len(data)
        serialize_int(self.server_socket, 0)
        self.server_socket.close()
        # print(f'Imagem "{image_name}" armazenada com sucesso em "{directory}/".')

    def start(self):
        """Inicializa o cliente"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.server_socket.connect(self.address)
        except Exception:
            print('[ERRO] Não foi possível estabelecer conexão com o servidor.')
            exit(0)
        print('[STATUS] Conexão com o servidor estabelecida.')

        images = ['img1.tiff', 'img2.tiff', 'img3.tiff', \
                  'img4.tiff', 'img5.tiff', 'img6.tiff', \
                  'img7.tiff', 'img8.tiff', 'img9.tiff', 'img10.tiff']

        print('Enviando imagens para o servidor...')
        for image_name in images:
            # print(f'\nRealizando upload da imagem {image_name}')
            image_path = os.path.join(self.IMAGES_DIR, image_name)
            if not os.path.exists(image_path):
                print(f'\n[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
                exit(0)
            serialize_int(self.server_socket, 1)
            self.upload_image(self.IMAGES_DIR, image_name)
            sleep(0.5)
        print('Todas as imagens foram armazenadas com sucesso.')
        serialize_int(self.server_socket, 0)
        self.server_socket.close()

        test_1_image = ['img1.tiff']
        test_5_images = ['img1.tiff', 'img2.tiff', \
                         'img3.tiff', 'img4.tiff', 'img5.tiff']
        test_10_images = ['img1.tiff', 'img2.tiff', 'img3.tiff', \
                        'img4.tiff', 'img5.tiff', 'img6.tiff', \
                        'img7.tiff', 'img8.tiff', 'img9.tiff', 'img10.tiff']
        test_cases = [test_1_image, test_5_images, test_10_images]

        test_cases = [test_5_images]

        print('\nBenchmark de upload')
        print('Realizando testes com 6 data nodes e fator de réplica 2')
        
        for test in test_cases:
            threads = []
        
            start_time = time() # Inicia o tempo de download

            for image_name in test:
                thread = threading.Thread(target=self.download_image, args=(self.IMAGES_DIR, image_name))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            end_time = time() # Termina o tempo de download

            print(f"\nTempo total de download de {len(test)} imagem(ns): {(end_time - start_time) * 1000:.3f} ms")
            sleep(1)
        
        # serialize_int(self.server_socket, 0)
        # self.server_socket.close()
        # print('\n[STATUS] Conexão com o servidor encerrada.')

if __name__ == '__main__':
    client = Client()
    client.start()
