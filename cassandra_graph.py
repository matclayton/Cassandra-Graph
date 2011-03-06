import pycassa

import time

POOL = pycassa.connect('Keyspace1', ['localhost:9160',])
EDGES = pycassa.ColumnFamily(POOL, 'Standard1')

EDGE_KEY = '%s:%s:%s'

# Read API's

def _get_edge_row_key(edge_type, node, forward):
    return EDGE_KEY % (edge_type, forward, str(node))

def get_edges(edge_type, node, forward=True):
    """
    Given a node, gets the edges that the node is connected to.
    """
    edge_row_key = _get_edge_row_key(edge_type, str(node), forward)
    edges = set(EDGES.get(edge_row_key))
    return edges

def get_edge_count(edge_type, node, forward=True):
    edge_row_key = _get_edge_row_key(edge_type, str(node), forward)
    return EDGES.get_count(edge_row_key)

# Write API's

def add_edges(edge_type, from_node, to_nodes):
    """
    Adds an edge relationship from one node to some others.
    """
    ts = str(int(time.time() * 1e6))
    dct = pycassa.util.OrderedDict(((str(node), ts) for node in to_nodes))

    batch = EDGES.batch()
    # Do forward insert
    forward_row_key = _get_edge_row_key(edge_type, str(from_node), True)

    batch.insert(forward_row_key, dct)
    # Setup all the reverse index
    for to_node in to_nodes:
        reversed_row_key = _get_edge_row_key(edge_type, str(to_node), False)
        batch.insert(reversed_row_key, {str(from_node): ts})
    # Insert the batch
    batch.send()

def remove_edges(edge_type, from_node, to_nodes):
    """
    Removes edge relationship from one node to some others.
    """
    batch = EDGES.batch()

    forward_row_key = _get_edge_row_key(edge_type, str(from_node), True)
    columns = [str(to_node) for to_node in to_nodes]
    batch.remove(forward_row_key, columns=columns)

    for to_node in to_nodes:
        reverse_row_key = _get_edge_row_key(edge_type, str(to_node), False)
        batch.remove(reverse_row_key, columns=[from_node,])

    batch.send()