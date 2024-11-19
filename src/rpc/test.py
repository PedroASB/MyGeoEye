def exposed_init_upload_image_chunk(self, image_name, image_size):
    self.current_image_name = image_name
    self.current_image_size = image_size
    self.current_image_size_division = math.ceil(image_size / SHARD_SIZE)
    self.current_image_part = 0
    self.current_data_nodes_index = 0
    self.current_image_accumulated_size = 0
    
    # Seleciona os nós para o armazenamento
    nodes_to_store = self.cluster.select_nodes_to_store()
    self.round_robin_nodes = cycle(nodes_to_store)
    
    # Prepara uma estrutura persistente para armazenar os nós de cada parte
    self.image_part_nodes = {}

    # Seleciona os primeiros nós para a parte inicial da imagem
    self.image_part_nodes[self.current_image_part] = list(
        islice(self.round_robin_nodes, self.REPLICATION_FACTOR)
    )
    
    print(f'image_size = {image_size}, division = {image_size / SHARD_SIZE}')
    print(f'current_image_size_division = {self.current_image_size_division}')
    print('data nodes = ')
    for i in range(self.cluster.cluster_size):
        print(nodes_to_store[i], end=' ')
    print()



def exposed_upload_image_chunk(self, image_chunk):
    self.current_image_accumulated_size += CHUNK_SIZE
    
    # Muda para a próxima parte se necessário
    if self.current_image_accumulated_size > SHARD_SIZE:
        self.current_image_accumulated_size = 0
        self.current_image_part += 1
        self.current_data_nodes_index += self.REPLICATION_FACTOR
        
        # Seleciona os próximos nós para a nova parte da imagem
        self.image_part_nodes[self.current_image_part] = list(
            islice(self.round_robin_nodes, self.REPLICATION_FACTOR)
        )

    # Recupera os nós persistentes para a parte atual
    current_data_nodes = self.image_part_nodes[self.current_image_part]
    print('current_data_nodes:', end=' ')
    for n in current_data_nodes:
        print(n, end=' ')
    print()

    # Envia o chunk para os nós selecionados
    for node_id in current_data_nodes:
        print()
        node = self.cluster.data_nodes[node_id]
        node['conn'].root.store_image_chunk(
            self.current_image_name, 
            self.current_image_part, 
            image_chunk
        )
        self.cluster.update_index_table(
            self.current_image_name,
            self.current_image_part,
            node_id,
            self.current_image_size_division
        )
