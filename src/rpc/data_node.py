import rpyc
import os
import psutil
from rpyc.utils.server import ThreadedServer

PORT_DATA_NODE = 8000
CHUNK_SIZE = 65_536 # 64 KB

class DataNode(rpyc.Service):
    STORAGE_DIR = 'data_node_storage'

    def __init__(self):
        if not os.path.exists(self.STORAGE_DIR):
            os.makedirs(self.STORAGE_DIR)


    def on_connect(self, conn):
        print("[STATUS] Data node conectado.")


    def on_disconnect(self, conn):
        print("[STATUS] Data node desconectado.")


    def exposed_get_node_status(self):
        """Retorna o status dos recursos do nó."""
        cpu_usage = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory().percent
        disk_info =  psutil.disk_usage('/').percent
        return {'cpu': cpu_usage, 'memory': memory_info, 'disk': disk_info}


    def exposed_store_image_chunk(self, image_name, image_chunk):
        # image_path = os.path.join(self.STORAGE_DIR, image_name)
        # with open(image_path, 'wb') as file:
        #     file.write(image_data)
        # print(f'[STATUS] Imagem "{image_name}" armazenada com sucesso.')
        
        # Cria o diretório onde os chunks serão salvos, se necessário
        image_path = os.path.join(self.STORAGE_DIR, image_name)
        # Armazena o chunk recebido
        with open(image_path, "ab") as file:
            file.write(image_chunk)


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
    
    
    def start(self):
        threaded_data_node = ThreadedServer(service=DataNode, port=PORT_DATA_NODE)
        threaded_data_node.start()
        print("[STATUS] Data node iniciado.")


if __name__ == "__main__":
    data_node = DataNode()
    # data_node.start()
    status = data_node.exposed_get_node_status()
    print(status)
