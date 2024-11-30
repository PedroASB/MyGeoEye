import sys
import rpyc
import time
import os

# Servidor
NAME_SERVER = 'geoeye_images'

# Serviço de nomes
HOST_NAME_SERVICE = 'localhost'
PORT_NAME_SERVICE = 6000

# Tamanho de chunks
CHUNK_SIZE = 65_536 # 64 KB

class Client:
    UPLOAD_DIR = 'client_uploads'
    DOWNLOAD_DIR = 'client_downloads'

    def __init__(self):
        self.server_conn = None
        if not os.path.exists(self.UPLOAD_DIR):
            os.makedirs(self.UPLOAD_DIR)
        if not os.path.exists(self.DOWNLOAD_DIR):
            os.makedirs(self.DOWNLOAD_DIR)
        self.clear_donwload_dir() # temporário


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
        attempt, error_msg = self.server_conn.root.init_upload_image_chunk(image_name, image_size)
        if not attempt:
            print(error_msg)
            return
        with open(image_path, "rb") as file:
            while image_chunk := file.read(CHUNK_SIZE):
                self.server_conn.root.upload_image_chunk(image_chunk)
        print(f'[STATUS] Imagem "{image_name}" enviada ao servidor.')


    def download_image(self, image_name):
        image_path = os.path.join(self.DOWNLOAD_DIR, image_name)
        if os.path.exists(image_path):
            print(f'[ERRO] Arquivo de imagem "{image_name}" já existente.')
            return
        attempt, error_msg, image_size = self.server_conn.root.init_download_image_chunk(image_name)
        if not attempt:
            print(error_msg)
            return
        print(f'Tamanho total: {(image_size / 2**10):.2f} KB')
        with open(image_path, 'ab') as file:
            received_size = 0
            while received_size < image_size:
                image_chunk = self.server_conn.root.download_image_chunk()
                if image_chunk == 'error':
                    print('Erro!')
                    break
                if image_chunk is None:
                    break
                file.write(image_chunk)
                received_size += len(image_chunk)
        print(f'Imagem "{image_name}" armazenada com sucesso em "{self.DOWNLOAD_DIR}/".')


    def list_images(self):
        images_list = self.server_conn.root.list_images()
        if len(images_list) == 0:
            print('\nNão há imagens armazenadas.')
        else:
            print('\nLista de imagens:')
            for image in images_list:
                print(image)


    def delete_image(self, image_name):
        """Deleta uma imagem"""
        if self.server_conn.root.delete_image(image_name):
            print(f'[STATUS] Imagem "{image_name}" deletada no servidor.')
        else:
            print('[ERRO] Imagem não encontrada no servidor.')
    

    def start(self, name_server):
        try:
            name_service_conn = rpyc.connect(HOST_NAME_SERVICE, PORT_NAME_SERVICE)
        except ConnectionRefusedError:
            print('[ERRO] Não foi possível estabelecer conexão com o serviço de nomes.')
            return False
        
        if not (server_registry := name_service_conn.root.lookup(name_server)):
            print(f'[ERRO] Servidor "{name_server}" não registrado no serviço de nomes.')
            return False
        
        host_server, port_server = server_registry
        while not self.server_conn:
            try:
                self.server_conn = rpyc.connect(host_server, port_server)
                print('[STATUS] Conexão com servidor estabelecida.')
                return True
            except ConnectionRefusedError:
                print("[STATUS] Conexão recusada. Tentando novamente em 5 segundos...")
                self.server_conn = None
                time.sleep(5)


    def handle_commands(self):
        if not self.server_conn:
            return
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
        self.server_conn.close()
        print('\n[STATUS] Conexão com o servidor encerrada.')


if __name__ == "__main__":
    client = Client()
    if client.start(name_server=NAME_SERVER):
        client.handle_commands()
    else:
        sys.exit(0)
