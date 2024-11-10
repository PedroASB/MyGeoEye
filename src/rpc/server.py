import rpyc
from rpyc.utils.server import ThreadedServer
from cluster import *
from addresses import *


PORT_SERVER = 5000
CHUNK_SIZE = 65_536 # 64 KB

class Server(rpyc.Service):
    DATA_NODES = [DATA_NODE_1, DATA_NODE_2, DATA_NODE_3]
    REPLICATION_FACTOR = 2 # Fator de réplica

    def __init__(self):
        self.cluster = Cluster(self.DATA_NODES, self.REPLICATION_FACTOR)
        self.cluster.connect_cluster()
        print('[STATUS] Servidor inicializado com o cluster.')


    def on_connect(self, conn):
        print("[STATUS] Cliente conectado.")


    def on_disconnect(self, conn):
        print("[STATUS] Cliente desconectado.")


    def exposed_send_chunk(self, chunk):
        pass


    def exposed_receive_chunk(self, chunk):
        pass


    def exposed_upload_image(self, image_name, image_data):
        print(f"[STATUS] Salvando imagem '{image_name}'.")

        selected_nodes = self.cluster.select_nodes_to_store()
        print('[INFO] Data nodes selecionados para armazenamento: ', end='')
        for node in selected_nodes:
            print(node, end=' ')

        for node_id in selected_nodes:
            data_node_conn = self.cluster.data_node_id[node_id][1]
            self.cluster.update_index_table(image_name, node_id)
            # store_img do data node
            data_node_conn.root.store_image(image_name, image_data)


    def exposed_download_image(self, image_name):
        if image_name in self.cluster.index_table:
            has_image = True
        else:
            has_image = False
        
        if not has_image:
            return
        
        # Selecionado o data node para realizar o upload
        node_id = self.cluster.select_node_to_retrieve(image_name)
        data_node_conn = self.cluster.data_node_id[node_id][1]

        print('[INFO] Data node selecionado para download:', node_id)
        
        image_data = data_node_conn.root.retrieve_image(image_name)

        return image_data


    def exposed_list_images(self):
        """Lista todas as imagens que estão armazenadas"""
        # for image_name in self.cluster.index_table:
        #     return image_name
        return list(self.cluster.index_table.keys())


    def exposed_delete_image(self, image_name):
        """Deleta uma imagem"""
        if image_name in self.cluster.index_table:
            has_image = True
            # img_01  ->  [[3 4], 1]
            print(f'[INFO] Deletando a imagem {image_name} de: ', end='')
            for node_id in self.cluster.index_table[image_name][0]:
                print(f'{node_id}', end=' ')
                data_node_conn = self.cluster.data_node_id[node_id][1] # 3 4
                data_node_conn.root.delete_image(image_name)
            del self.cluster.index_table[image_name]
        else:
            has_image = False

        return has_image


if __name__ == "__main__":
    server = ThreadedServer(service=Server, port=PORT_SERVER)
    server.start()
    print("[STATUS] Servidor iniciado.")
