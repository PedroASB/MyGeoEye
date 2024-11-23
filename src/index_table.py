class IndexTable:
    def __init__(self):
        self.shard_nodes = {}  # image_name -> [shard_0_nodes, shard_1_nodes, ...]
        self.shard_size = {}   # image_name -> [shard_0_size, shard_1_size, ...]
        self.image_size = {}   # image_name -> total_size


    def init_update_image(self, image_name, image_size_division):
        if image_name in self.shard_nodes:
            return False
        self.shard_nodes[image_name] = [[] for _ in range(image_size_division)]
        self.shard_size[image_name] = [None for _ in range(image_size_division)]
        self.image_size[image_name] = 0
        return True


    def update_image(self, image_name, shard_index, shard_size, nodes_id):
        """Atualiza a tabela de índices para uma imagem dividida em partes."""
        for node_id in nodes_id:
            if node_id not in self.shard_nodes[image_name][shard_index]:
                self.shard_nodes[image_name][shard_index].append(node_id)
        self.shard_size[image_name][shard_index] = shard_size
        #self.image_size[image_name] = sum(size for size in self.shard_size[image_name] if size is not None)
        self.image_size[image_name] += shard_size


    def print(self):
        # shard_nodes: image_name -> [shard_0_nodes: [1 2 3], shard_1_nodes: [4 5 6], ...]
        for image_name, shards in self.shard_nodes.items():
            print(f'{image_name}:')
            for shard_index, nodes in enumerate(shards):
                size = self.shard_size[image_name][shard_index]
                print(f'  Shard {shard_index}: nodes={nodes}, size={size}')


    def get_shards(self, image_name):
        return [{
                    'nodes': self.shard_nodes[image_name][shard_index], \
                    'size': self.shard_size[image_name][shard_index] \
                }
                for shard_index in range(len(self.shard_nodes[image_name]))]


    def get_nodes_from_shard(self, image_name, shard_index):
        return self.shard_nodes[image_name][shard_index]


    def get_shard_size(self, image_name, shard_index):
        return self.shard_size[image_name][shard_index]


    def get_image_size(self, image_name):
        return self.image_size.get(image_name, 0)


    def image_total_size(self, image_name):
        return self.get_image_size(image_name)
