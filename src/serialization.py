from struct import pack, unpack

FORMAT = 'utf-8'
SIZE_OF_INT = 4
SIZE_OF_LONG = 8

def deserialize_int(connection):
    return unpack('!I', connection.recv(SIZE_OF_INT))[0]

def deserialize_long(connection):
    return unpack('!Q', connection.recv(SIZE_OF_LONG))[0]

def deserialize_string(connection):
	string_lenght = deserialize_int(connection)
	return connection.recv(string_lenght).decode(FORMAT)

def serialize_int(connection, number):
    connection.send(pack('!I', number))

def serialize_long(connection, number):
    connection.send(pack('!Q', number))

def serialize_string(connection, string):
    serialize_int(connection, len(string))
    connection.send(string.encode(FORMAT))
