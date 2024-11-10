import rpyc
import time
import os

IP_SERVER = "localhost"
PORT_SERVER = 5000
CHUNK_SIZE = 65_536 # 64 KB

class Client:
    IMAGES_DIR = 'client_images'

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER):
        self.ip = ip
        self.port = port
        self.client_conn = None
        if not os.path.exists(self.IMAGES_DIR):
            os.makedirs(self.IMAGES_DIR)


    def on_connect(self, conn):
        print("[STATUS] Conexão com o servidor estabelecida.")


    def on_disconnect(self, conn):
        print("[STATUS] Conexão com o servidor encerrada.")
        

    def upload_image(self, image_name):
        """Envia uma imagem para o servidor"""
        image_path = os.path.join(self.IMAGES_DIR, image_name)
        if not os.path.exists(image_path):
            print(f'[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
            return
        with open(image_path, "rb") as file:
            image_data = file.read()
        self.client_conn.root.upload_image(image_name, image_data)
        # with open(image_path, "rb") as file:
        #     while image_chunk := file.read(CHUNK_SIZE):
        #         self.client_conn.root.upload_image_chunk(image_name, image_chunk)
        # print(f'[STATUS] Imagem "{image_name}" enviada ao servidor.')


    def download_image(self, image_name):
        # """Baixa uma imagem do servidor"""
        # serialize_string(self.client_socket, image_name)
        # has_image = deserialize_bool(self.client_socket)
        # if not has_image:
        #     print(f'\n[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
        #     return
        # image_size = deserialize_int(self.client_socket)
        # image_path = os.path.join(directory, image_name)
        # with open(image_path, 'wb') as file:
        #     received_size = 0
        #     while received_size < image_size:
        #         data = self.client_socket.recv(CHUNK_SIZE)
        #         file.write(data)
        #         received_size += len(data)
        # print(f'Imagem "{image_name}" armazenada com sucesso em "{directory}/".')
        image_data = self.client_conn.root.download_image(image_name)
        if image_data:
            with open(os.path.join(self.IMAGES_DIR, image_name), 'wb') as file:
                file.write(image_data)
            print(f'[STATUS] Imagem "{image_name}" baixada com sucesso.')
        else:
            print('[ERRO] Imagem não encontrada no servidor.')


    def list_images(self):
        images_list = self.client_conn.root.list_images()
        if len(images_list) == 0:
            print('\nNão há imagens armazenadas.')
        else:
            print('\nLista de imagens:')
            for image in images_list:
                print(image)


    def delete_image(self, image_name):
        """Deleta uma imagem"""
        if self.client_conn.root.delete_image(image_name):
            print(f'[STATUS] Imagem "{image_name}" deletada no servidor.')
        else:
            print('[ERRO] Imagem não encontrada no servidor.')
    
    
    def start(self):
        while not self.client_conn:
            try:
                self.client_conn = rpyc.connect(self.ip, self.port)
                # print('[STATUS] Conectado ao servidor.')
            except ConnectionRefusedError:
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                self.client_conn = None
                time.sleep(5)

        while True:
            print('\nSelecione um comando:')
            print('1 - Enviar imagem')
            print('2 - Baixar imagem')
            print('3 - Listar imagens')
            print('4 - Deletar imagem')
            print('0 - Finalizar')
            command = input('>> ')
            match command:
                case '1':
                    image_name = str(input('\nImagem a ser enviada: ')).strip()
                    self.upload_image(image_name)
                    
                case '2':
                    image_name = str(input('\nImagem a ser baixada: ')).strip()
                    self.download_image(image_name)

                case '3':
                    self.list_images()

                case '4':
                    image_name = str(input('\nImagem a ser deletada: ')).strip()
                    self.delete_image(image_name)

                case '0':
                    break

                case _:
                    print('\n[ERRO] Comando inválido.')
        self.client_conn.close()
        # print('[STATUS] Conexão encerrada.')


if __name__ == "__main__":
    client = Client()
    client.start()
