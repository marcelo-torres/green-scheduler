import unittest

from src.scheduling.model.create_graph_exception import CreateGraphException
from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from tests.scheduling.graph_utils import get_pipeline_graph, get_stencil_graph, get_multidependency_graph, \
    get_parallel_graph


class TopologicalOrderingTest(unittest.TestCase):

    def test_calc_critical_path_no_task(self):
        graph = TaskGraph()
        with self.assertRaises(CreateGraphException):
            calc_critical_path_length(graph)

    def test_calc_critical_path_single_task(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, 36343)
        graph.set_start_task(task.id)
        length = calc_critical_path_length(graph)
        self.assertEqual(task.runtime, length)

    def test_topological_ordering_pipeline_graph(self):
        graph = get_pipeline_graph()
        length = calc_critical_path_length(graph)
        self.assertEqual(30, length)

    def test_topological_ordering_paralell_graph(self):
        graph = get_parallel_graph()
        length = calc_critical_path_length(graph)
        self.assertEqual(4+10+7+2, length)

    def test_topological_ordering_stencil_graph(self):
        graph = get_stencil_graph()
        length = calc_critical_path_length(graph)
        self.assertEqual(4+10+7+3+15, length)

    def test_topological_ordering_multidependency_graph(self):
        graph = get_multidependency_graph()
        length = calc_critical_path_length(graph)
        self.assertEqual(4+10+8+10+7+10+2, length)

    def test_topological_ordering(self):
        graph = TaskGraph()
        graph.set_start_task(0)
        graph.add_new_task(0, runtime=0, power=0)  # Dummy task
        graph.add_new_task(1, runtime=10, power=14)
        graph.add_new_task(2, runtime=15, power=10)
        graph.add_new_task(3, runtime=20, power=12)
        graph.add_new_task(4, runtime=7, power=18)
        graph.add_new_task(5, runtime=14, power=14)
        graph.add_new_task(6, runtime=12, power=16)
        graph.add_new_task(7, runtime=8, power=4)

        graph.create_dependency(0, 1)
        graph.create_dependency(0, 2)
        graph.create_dependency(1, 3)
        graph.create_dependency(2, 3)
        graph.create_dependency(3, 6)
        graph.create_dependency(3, 4)
        graph.create_dependency(3, 5)
        graph.create_dependency(4, 6)
        graph.create_dependency(4, 7)
        graph.create_dependency(5, 7)
        graph.create_dependency(6, 7)

        critical_path_length = calc_critical_path_length(graph)

        self.assertEqual(62, critical_path_length)
