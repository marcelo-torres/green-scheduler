
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

#matplotlib.use("qt5agg") # https://stackoverflow.com/a/52221178

def draw_task_graph(graph):

    G = _create_nx_graph(graph)

    for layer, nodes in enumerate(nx.topological_generations(G)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            G.nodes[node]["layer"] = layer
            print(layer)

    # Compute the multipartite_layout using the "layer" node attribute
    pos = nx.multipartite_layout(G, subset_key="layer")

    fig, ax = plt.subplots()
    nx.draw_networkx(G, pos=pos, ax=ax, with_labels=False)
    ax.set_title("DAG layout in topological order")
    fig.tight_layout()
    plt.show()


def _create_nx_graph(graph):

    edges = []

    for task in graph.list_of_tasks():
        for successor in task.successors:
            edges.append(
                (task.id, successor.id)
            )

    G = nx.DiGraph(edges)

    for task in graph.list_of_tasks():
        G.nodes[task.id]['runtime'] = task.runtime
        G.nodes[task.id]['power'] = task.power

    return G
