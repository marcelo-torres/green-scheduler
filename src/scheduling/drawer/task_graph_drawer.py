
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use(matplotlib.get_backend()) #https://stackoverflow.com/a/52221178

def draw_task_graph(graph, with_labels=False, schedule=None, current_task_id=None):

    G, node_colors = _create_nx_graph(graph, schedule=schedule, current_task_id=current_task_id)

    for layer, nodes in enumerate(nx.topological_generations(G)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            G.nodes[node]["layer"] = layer

    # Compute the multipartite_layout using the "layer" node attribute
    pos = nx.multipartite_layout(G, subset_key="layer")

    fig, ax = plt.subplots()
    nx.draw_networkx(G, pos=pos, ax=ax, with_labels=with_labels, node_color=node_colors)
    ax.set_title("DAG layout in topological order")
    fig.tight_layout()
    plt.show()


def _create_nx_graph(graph, schedule=None, current_task_id=None):

    edges = []

    for task in graph.list_of_tasks():
        for successor in task.successors:
            edges.append(
                (task.id, successor.id)
            )

    G = nx.DiGraph(edges)
    node_colors = []

    for task in graph.list_of_tasks():
        G.nodes[task.id]['runtime'] = task.runtime
        G.nodes[task.id]['power'] = task.power

        if task.id == current_task_id:
            color = 'tab:orange' if schedule and task.id in schedule else 'tab:red'
        else:
            color = 'tab:green' if schedule and task.id in schedule else 'tab:blue'

        node_colors.append(color)

    return G, node_colors
