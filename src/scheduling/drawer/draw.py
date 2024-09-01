
# Input: graph of tasks
# Green Energy Data

# 1) Order all tasks by energy usage (power * runtime)
# 2) For each task:
# 2.1)  Calculate boundaries to avoid that a single task gets all slack time
# 2.2) Schedule each task when it uses less brown energy as early as possible

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

#matplotlib.use("qt5agg") # https://stackoverflow.com/a/52221178


#matplotlib.get_backend()

def create_graph():
    G = nx.DiGraph(
        [
            ("1", "3"),
            ("2", "3"),
            ("3", "4"),
            ("3", "5"),
            ("4", "6"),
            ("5", "7"),
            ("6", "7"),
        ]
    )

    G.nodes["1"]["runtime"] = 10
    G.nodes["1"]["power"] = 14

    G.nodes["2"]["runtime"] = 15
    G.nodes["2"]["power"] = 10

    G.nodes["3"]["runtime"] = 20
    G.nodes["3"]["power"] = 12

    G.nodes["4"]["runtime"] = 7
    G.nodes["4"]["power"] = 18

    G.nodes["5"]["runtime"] = 14
    G.nodes["5"]["power"] = 14

    G.nodes["6"]["runtime"] = 12
    G.nodes["6"]["power"] = 16

    G.nodes["7"]["runtime"] = 8
    G.nodes["7"]["power"] = 4

    return G

def schedule_graph(G):

    nodes_by_power = sorted(G.nodes, key=lambda k: G.nodes[k]["power"], reverse=True)

    for node in nodes_by_power:
        pass


# print(nodes_by_power)
# print(
#     list(
#         map(lambda k: G.nodes[k]["power"], nodes_by_power)
#     )
# )

G = create_graph()
schedule_graph(G)

# for layer, nodes in enumerate(nx.topological_generations(G)):
#     # `multipartite_layout` expects the layer as a node attribute, so add the
#     # numeric layer value as a node attribute
#     for node in nodes:
#         G.nodes[node]["layer"] = layer
#         print(layer)
#
# # Compute the multipartite_layout using the "layer" node attribute
# pos = nx.multipartite_layout(G, subset_key="layer")
#
# fig, ax = plt.subplots()
# nx.draw_networkx(G, pos=pos, ax=ax)
# ax.set_title("DAG layout in topological order")
# fig.tight_layout()
# plt.show()