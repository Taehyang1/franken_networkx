"""Layout algorithms — delegates to NetworkX after graph conversion."""


def _to_nx(G):
    """Convert a FrankenNetworkX graph to a NetworkX graph for drawing."""
    import networkx as nx

    if G.is_directed():
        H = nx.DiGraph()
    else:
        H = nx.Graph()
    for n in G.nodes():
        H.add_node(n)
    for u, v, d in G.edges(data=True):
        H.add_edge(u, v, **d)
    return H


def spring_layout(G, **kwargs):
    """Position nodes using Fruchterman-Reingold force-directed algorithm."""
    import networkx as nx
    return nx.spring_layout(_to_nx(G), **kwargs)


def circular_layout(G, **kwargs):
    """Position nodes on a circle."""
    import networkx as nx
    return nx.circular_layout(_to_nx(G), **kwargs)


def random_layout(G, **kwargs):
    """Position nodes uniformly at random."""
    import networkx as nx
    return nx.random_layout(_to_nx(G), **kwargs)


def shell_layout(G, **kwargs):
    """Position nodes in concentric circles."""
    import networkx as nx
    return nx.shell_layout(_to_nx(G), **kwargs)


def spectral_layout(G, **kwargs):
    """Position nodes using eigenvectors of the graph Laplacian."""
    import networkx as nx
    return nx.spectral_layout(_to_nx(G), **kwargs)


def kamada_kawai_layout(G, **kwargs):
    """Position nodes using Kamada-Kawai path-length cost function."""
    import networkx as nx
    return nx.kamada_kawai_layout(_to_nx(G), **kwargs)


def planar_layout(G, **kwargs):
    """Position nodes without edge crossings (if graph is planar)."""
    import networkx as nx
    return nx.planar_layout(_to_nx(G), **kwargs)
