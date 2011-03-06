r"""
>>> from cassandra_graph import *

>>> add_edges('like', 'a', ['b','c'])
>>> get_edge_count('like', 'a')
2
>>> get_edges('like', 'a')
set(['c', 'b'])
>>> remove_edges('like', 'a', ['b',])
>>> get_edge_count('like', 'a')
1
>>> get_edges('like', 'a')
set(['c'])

>>> get_edge_count('listen', 'a')
0
>>> add_edges('listen', 'a', ['d','e'])
>>> get_edge_count('listen', 'a')
2
>>> remove_edges('listen', 'a', ['d','e'])
>>> get_edge_count('listen', 'a')
0

>>> add_edges('test', 'b', [a for a in range(1,1000)])
>>> get_edge_count('test', 'b')
999
>>> remove_edges('test', 'b', [a for a in range(1,1000)])
>>> get_edge_count('test', 'b')
0
"""

if __name__ == '__main__':
    import doctest
    doctest.testmod()
