from src.data.photovolta import PhotovoltaReader
from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.model.cluster import create_single_machine_cluster, Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.model.task_graph import TaskGraph


def _get_graph():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=10, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=7, power=10)
    graph.add_new_task(3, runtime=2, power=10)
    graph.add_new_task(4, runtime=4, power=10)
    graph.add_new_task(5, runtime=8, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    graph.create_dependency(2, 5)
    graph.create_dependency(3, 5)
    graph.create_dependency(4, 5)

    return graph, 34


def run_single_test():
    #green_power = [5, 3, 2, 1, 4, 5, 20, 40, 30, 20, 10, 6, 8, 5]
    green_power = [5, 4, 20, 40, 30, 50, 40, 45, 8, 5]
    power_series = PowerSeries('g1', green_power, 10)

    cluster = Cluster('c1', power_series, [
        #Machine('m1', 1, 0),
        Machine('m2', 5, 0)
    ])

    graph, min_makespan = _get_graph()

    draw_task_graph(graph, with_labels=True)

    scheduling = highest_power_first(graph, min_makespan * 4, 0.3, [cluster],
                                         task_sort='energy',
                                         shift_mode='none',
                                         show='last',
                                         max_power=None
                                     )


if __name__ == '__main__':
    run_single_test()
