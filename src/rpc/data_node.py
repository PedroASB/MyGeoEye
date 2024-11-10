import rpyc
import os
from rpyc.utils.server import ThreadedServer

PORT_DATA_NODE = 8000
CHUNK_SIZE = 65_536 # 64 KB

class DataNode(rpyc.Service):
    STORAGE_DIR = 'data_node_storage'

    def __init__(self):
        if not os.path.exists(self.STORAGE_DIR):
            os.makedirs(self.STORAGE_DIR)
        print("[STATUS] Data node inicializado com o diretório de armazenamento.")


    def on_connect(self, conn):
        print("[STATUS] Data node conectado.")


    def on_disconnect(self, conn):
        print("[STATUS] Data node desconectado.")


    def exposed_store_image(self, image_name, image_data):
        image_path = os.path.join(self.STORAGE_DIR, image_name)
        with open(image_path, 'wb') as file:
            file.write(image_data)
        print(f'[STATUS] Imagem "{image_name}" armazenada com sucesso.')


    def exposed_retrieve_image(self, image_name):
        image_path = os.path.join(self.STORAGE_DIR, image_name)
        if os.path.exists(image_path):
            print(f'[STATUS] Recuperando imagem "{image_name}".')
            with open(image_path, 'rb') as file:
                return file.read()
        print(f'[STATUS] Imagem "{image_name}" não encontrada.')
        return None


    def exposed_delete_image(self, image_name):
        image_path = os.path.join(self.STORAGE_DIR, image_name)
        if os.path.exists(image_path):
            os.remove(image_path)
            print(f'[STATUS] Imagem "{image_name}" deletada com sucesso.')
        else:
            print(f'[STATUS] Imagem "{image_name}" não encontrada para deletar.')


if __name__ == "__main__":
    server = ThreadedServer(service=DataNode, port=PORT_DATA_NODE)
    print("[STATUS] Data node RPyC iniciado.")
    server.start()
