import unittest

from src.scheduling.algorithms.task_flow.task_flow import task_flow_schedule
from src.scheduling.model.cluster import create_single_machine_cluster
from src.scheduling.model.task_graph import TaskGraph


def _get_graph_1():

    graph = TaskGraph()
    graph.set_start_task(0)

    graph.add_new_task(0, runtime=0, power=10)
    graph.add_new_task(1, runtime=10, power=10)
    graph.add_new_task(2, runtime=15, power=15)
    graph.add_new_task(3, runtime=20, power=10)
    graph.add_new_task(4, runtime=7, power=30)
    graph.add_new_task(5, runtime=14, power=20)
    graph.add_new_task(6, runtime=12, power=20)
    graph.add_new_task(7, runtime=8, power=20)

    graph.create_dependency(0, 1)
    graph.create_dependency(0, 2)

    graph.create_dependency(1, 3)
    graph.create_dependency(2, 3)

    graph.create_dependency(3, 4)
    graph.create_dependency(3, 5)
    graph.create_dependency(3, 6)

    graph.create_dependency(4, 6)

    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph

class TaskFlowTest(unittest.TestCase):

    def test_single_task_with_no_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        cluster = create_single_machine_cluster([], 0)
        scheduling = task_flow_schedule(graph, [cluster])

        self.assertEqual(0, scheduling[task.id])

    def test_single_task_with_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        cluster = create_single_machine_cluster([0, 0, 0, 1000], 100)
        scheduling = task_flow_schedule(graph, [cluster])

        self.assertEqual(0, scheduling[task.id])

    def test_default_graph_no_energy(self):
        graph = _get_graph_1()
        cluster = create_single_machine_cluster([], 0)
        scheduling = task_flow_schedule(graph, [cluster])

        self.assertEqual(0, scheduling[0])
        self.assertEqual(0, scheduling[1])
        self.assertEqual(0, scheduling[2])
        self.assertEqual(15, scheduling[3])
        self.assertEqual(35, scheduling[4])
        self.assertEqual(35, scheduling[5])
        self.assertEqual(42, scheduling[6])
        self.assertEqual(54, scheduling[7])

    def test_default_graph_energy(self):
        graph = _get_graph_1()
        cluster = create_single_machine_cluster([5, 30, 20, 30, 10, 40, 20], 10)
        scheduling = task_flow_schedule(graph, [cluster])

        self.assertEqual(0, scheduling[0])
        self.assertEqual(5, scheduling[1])
        self.assertEqual(0, scheduling[2])
        self.assertEqual(15, scheduling[3])
        self.assertEqual(35, scheduling[4])
        self.assertEqual(40, scheduling[5])
        self.assertEqual(42, scheduling[6])
        self.assertEqual(54, scheduling[7])

