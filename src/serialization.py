from struct import pack, unpack

FORMAT = 'utf-8'
SIZE_OF_INT = 4
SIZE_OF_LONG = 8
CHUNK_SIZE = 65_536 # 64 KB

def serialize_int(connection, number):
    """Serializa e envia um número do tipo int"""
    connection.send(pack('!I', number))

def serialize_long(connection, number):
    """Serializa e envia um número do tipo long"""
    connection.send(pack('!Q', number))

def serialize_string(connection, string):
    """Serializa e envia uma string"""
    serialize_int(connection, len(string))
    connection.send(string.encode(FORMAT))

def deserialize_int(connection):
    """Deserializa e retorna um número do tipo int"""
    return unpack('!I', connection.recv(SIZE_OF_INT))[0]

def deserialize_long(connection):
    """Deserializa e retorna um número do tipo long"""
    return unpack('!Q', connection.recv(SIZE_OF_LONG))[0]

def deserialize_string(connection):
    """Deserializa e retorna uma string"""
    string_lenght = deserialize_int(connection)
    return connection.recv(string_lenght).decode(FORMAT)

