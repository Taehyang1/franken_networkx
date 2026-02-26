"""Drawing functions — delegates to NetworkX/matplotlib after graph conversion."""

from franken_networkx.drawing.layout import _to_nx


def draw(G, pos=None, ax=None, **kwargs):
    """Draw the graph G with matplotlib.

    Converts the FrankenNetworkX graph to a NetworkX graph and delegates
    to ``networkx.draw``.
    """
    import networkx as nx
    nx.draw(_to_nx(G), pos=pos, ax=ax, **kwargs)


def draw_spring(G, **kwargs):
    """Draw with spring layout."""
    import networkx as nx
    nx.draw_spring(_to_nx(G), **kwargs)


def draw_circular(G, **kwargs):
    """Draw with circular layout."""
    import networkx as nx
    nx.draw_circular(_to_nx(G), **kwargs)


def draw_random(G, **kwargs):
    """Draw with random layout."""
    import networkx as nx
    nx.draw_random(_to_nx(G), **kwargs)


def draw_spectral(G, **kwargs):
    """Draw with spectral layout."""
    import networkx as nx
    nx.draw_spectral(_to_nx(G), **kwargs)


def draw_shell(G, **kwargs):
    """Draw with shell layout."""
    import networkx as nx
    nx.draw_shell(_to_nx(G), **kwargs)


def draw_kamada_kawai(G, **kwargs):
    """Draw with Kamada-Kawai layout."""
    import networkx as nx
    nx.draw_kamada_kawai(_to_nx(G), **kwargs)


def draw_planar(G, **kwargs):
    """Draw with planar layout (if graph is planar)."""
    import networkx as nx
    nx.draw_planar(_to_nx(G), **kwargs)
