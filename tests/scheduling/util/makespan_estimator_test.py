import unittest

from src.scheduling.model.machine import Machine
from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.makespan_estimator import estimate_min_makespan

'''
    1
'''
def _get_graph_1():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=3, power=10)
    graph.set_start_task(task.id)

    return graph, 3

'''
    1 -> 2 -> 3 -> 4 -> 5
'''
def _get_graph_2():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=3, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=70, power=10)
    graph.add_new_task(3, runtime=5, power=20)
    graph.add_new_task(4, runtime=1, power=10)
    graph.add_new_task(5, runtime=8, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(2, 3)
    graph.create_dependency(3, 4)
    graph.create_dependency(4, 5)

    return graph, 87

'''
        +-> 2 -+             +-> 6 -+
       /         \          /        \
    1-+---> 3 ----+--> 5 --+---> 7 ---+--> 9
       \         /          \        /
        +-> 4 -+             +-> 8 -+
'''
def _get_graph_3():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=5, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=10, power=10)
    graph.add_new_task(3, runtime=60, power=10)
    graph.add_new_task(4, runtime=50, power=10)

    graph.add_new_task(5, runtime=1, power=10)

    graph.add_new_task(6, runtime=2, power=10)
    graph.add_new_task(7, runtime=3, power=10)
    graph.add_new_task(8, runtime=7, power=10)

    graph.add_new_task(9, runtime=13, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    graph.create_dependency(2, 5)
    graph.create_dependency(3, 5)
    graph.create_dependency(4, 5)

    graph.create_dependency(5, 6)
    graph.create_dependency(5, 7)
    graph.create_dependency(5, 8)

    graph.create_dependency(6, 9)
    graph.create_dependency(7, 9)
    graph.create_dependency(8, 9)

    return graph, 86


class MakespanEstimator(unittest.TestCase):

    def test_single_node_single_machine(self):
        graph, min_makespan = _get_graph_1()

        machine = Machine('m1', 1)

        estimated_makespan = estimate_min_makespan(graph, [machine])

        self.assertEqual(min_makespan, estimated_makespan)

    def test_single_node_multi_machine(self):
        graph, min_makespan = _get_graph_1()

        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 2)

        estimated_makespan = estimate_min_makespan(graph, [machine1, machine2])

        self.assertEqual(min_makespan, estimated_makespan)

    def test_pipeline_single_machine(self):
        graph, min_makespan = _get_graph_2()

        machine = Machine('m1', 1)

        estimated_makespan = estimate_min_makespan(graph, [machine])

        self.assertEqual(min_makespan, estimated_makespan)

    def test_pipeline_multi_machine(self):
        graph, min_makespan = _get_graph_2()

        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 2)

        estimated_makespan = estimate_min_makespan(graph, [machine1, machine2])

        self.assertEqual(min_makespan, estimated_makespan)

    def test_parallel_graph_with_enough_cores(self):
        graph, min_makespan = _get_graph_3()

        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 2)

        estimated_makespan = estimate_min_makespan(graph, [machine1, machine2])

        self.assertEqual(min_makespan, estimated_makespan)

    def test_parallel_graph_without_enough_cores(self):
        graph, min_makespan = _get_graph_3()

        machine1 = Machine('m1', 1)

        estimated_makespan = estimate_min_makespan(graph, [machine1])

        self.assertEqual(151, estimated_makespan)
