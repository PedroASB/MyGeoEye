import rpyc
import time
import os

IP_SERVER = "localhost"
PORT_SERVER = 5000
CHUNK_SIZE = 65_536 # 64 KB

class Client:
    UPLOAD_DIR = 'client_uploads'
    DOWNLOAD_DIR = 'client_downloads'

    def __init__(self, ip=IP_SERVER, port=PORT_SERVER):
        self.ip = ip
        self.port = port
        self.client_conn = None
        if not os.path.exists(self.UPLOAD_DIR):
            os.makedirs(self.UPLOAD_DIR)
        if not os.path.exists(self.DOWNLOAD_DIR):
            os.makedirs(self.DOWNLOAD_DIR)
        self.clear_donwload_dir()


    def on_connect(self, conn):
        pass


    def on_disconnect(self, conn):
        pass
        
    
    def clear_donwload_dir(self):
        """Remove todos os arquivos e subdiretórios do diretório de armazenamento."""
        if os.path.exists(self.DOWNLOAD_DIR):
            for root, dirs, files in os.walk(self.DOWNLOAD_DIR, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))


    def upload_image(self, image_name):
        """Envia uma imagem para o servidor"""
        image_path = os.path.join(self.UPLOAD_DIR, image_name)
        if not os.path.exists(image_path):
            print(f'[ERRO] Arquivo de imagem "{image_name}" não encontrado.')
            return
        image_size = os.path.getsize(image_path)
        attempt, error_msg = self.client_conn.root.init_upload_image_chunk(image_name, image_size)
        if not attempt:
            print(error_msg)
            return
        with open(image_path, "rb") as file:
            while image_chunk := file.read(CHUNK_SIZE):
                self.client_conn.root.upload_image_chunk(image_chunk)
        print(f'[STATUS] Imagem "{image_name}" enviada ao servidor.')


    def download_image(self, image_name):
        image_path = os.path.join(self.DOWNLOAD_DIR, image_name)
        if os.path.exists(image_path):
            print(f'[ERRO] Arquivo de imagem "{image_name}" já existente.')
            return
        attempt, error_msg, image_size = self.client_conn.root.init_download_image_chunk(image_name)
        if not attempt:
            print(error_msg)
            return
        print(f'image_size = {image_size} ({(image_size / 2**10):.2f} KB)')
        with open(image_path, 'ab') as file:
            received_size = 0
            while received_size < image_size:
                image_chunk = self.client_conn.root.download_image_chunk()
                if image_chunk == 'error':
                    print('Erro!')
                    break
                if image_chunk is None:
                    break
                file.write(image_chunk)
                received_size += len(image_chunk)
        print(f'Imagem "{image_name}" armazenada com sucesso em "{self.DOWNLOAD_DIR}/".')


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
                print('[STATUS] Conexão com servidor estabelecida.')
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
