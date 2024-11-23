import rpyc
import os
import io
import psutil
from rpyc.utils.server import ThreadedServer

PORT_DATA_NODE = 8000
CHUNK_SIZE = 65_536 # 64 KB

class DataNode(rpyc.Service):
    STORAGE_DIR = 'data_node_storage'

    def __init__(self):
        if not os.path.exists(self.STORAGE_DIR):
            os.makedirs(self.STORAGE_DIR)
        self.open_files = {}

    def exposed_clear_storage_dir(self):
        """Remove todos os arquivos e subdiretórios do diretório de armazenamento."""
        if os.path.exists(self.STORAGE_DIR):
            for root, dirs, files in os.walk(self.STORAGE_DIR, topdown=False):
                # Remove todos os arquivos
                for file in files:
                    os.remove(os.path.join(root, file))

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


    def exposed_store_image_chunk(self, image_name, image_part, image_chunk):
        # Cria o diretório onde os chunks serão salvos, se necessário
        image_part_name = f'{image_name}%part{image_part}%'
        image_path = os.path.join(self.STORAGE_DIR, image_part_name)
        # Armazena o chunk recebido
        with open(image_path, "ab") as file:
            file.write(image_chunk)


    def exposed_retrieve_image_chunk(self, image_name, image_part):
        """
        Envia chunks de uma parte de imagem para o servidor de forma incremental.
        """
        image_part_name = f'{image_name}%part{image_part}%'
        image_path = os.path.join(self.STORAGE_DIR, image_part_name)
        
        # Verificar se o arquivo já está aberto ou não
        if image_part_name not in self.open_files:
            if not os.path.exists(image_path):
                print(f"[ERRO] {image_name}%part{image_part}% não encontrada.")
                return None
            # Abrir o arquivo e armazenar no mapeamento
            self.open_files[image_part_name] = open(image_path, "rb")

        file = self.open_files[image_part_name]
        image_chunk = file.read(CHUNK_SIZE)
        eof = False
        
        # Significa que temos o último "chunk" ou vazio
        if len(image_chunk) < CHUNK_SIZE:
            file.close()
            del self.open_files[image_part_name]
            print(f"[STATUS] Leitura de {image_name}%part{image_part}% concluída.")
            eof = True
        
        for of in self.open_files:
            print(of)
        
        return image_chunk, eof


    def exposed_delete_image(self, image_name):
        image_path = os.path.join(self.STORAGE_DIR, image_name)
        if os.path.exists(image_path):
            os.remove(image_path)
            print(f'[STATUS] Imagem "{image_name}" deletada com sucesso.')
        else:
            print(f'[STATUS] Imagem "{image_name}" não encontrada para deletar.')
    
    
    def start(self):
        threaded_data_node = ThreadedServer(service=DataNode, port=PORT_DATA_NODE,
                                            protocol_config={'allow_public_attrs': True})
        threaded_data_node.start()
        print("[STATUS] Data node iniciado.")


if __name__ == "__main__":
    data_node = DataNode()
    data_node.start()
