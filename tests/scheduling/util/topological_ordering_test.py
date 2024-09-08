import unittest

from src.scheduling.task_graph.create_graph_exception import CreateGraphException
from src.scheduling.task_graph.task_graph import TaskGraph
from src.scheduling.util.topological_ordering import calculate_upward_rank, sort_topologically
from tests.scheduling.graph_utils import get_pipeline_graph, get_parallel_graph, get_stencil_graph, \
    get_multidependency_graph


class TopologicalOrderingTest(unittest.TestCase):

    def test_topological_ordering_no_task(self):
        graph = TaskGraph()
        with self.assertRaises(CreateGraphException):
            calculate_upward_rank(graph)

    def test_topological_ordering_single_task(self):
        graph = TaskGraph()
        task = graph.add_new_task(1)
        graph.set_start_task(task.id)
        ranks = calculate_upward_rank(graph)
        self.assertEqual(0, ranks[task.id])

    def test_topological_ordering_pipeline_graph(self):
        graph = get_pipeline_graph()
        ranks = calculate_upward_rank(graph)
        self.assertEqual(0, ranks[1])
        self.assertEqual(1, ranks[2])
        self.assertEqual(2, ranks[3])
        self.assertEqual(3, ranks[4])

    def test_topological_ordering_paralell_graph(self):
        graph = get_parallel_graph()
        ranks = calculate_upward_rank(graph)
        self.assertEqual(0, ranks[1])
        self.assertEqual(1, ranks[2])
        self.assertEqual(1, ranks[3])
        self.assertEqual(2, ranks[4])
        self.assertEqual(2, ranks[5])
        self.assertEqual(2, ranks[6])
        self.assertEqual(3, ranks[7])

    def test_topological_ordering_stencil_graph(self):
        graph = get_stencil_graph()
        ranks = calculate_upward_rank(graph)
        self.assertEqual(0, ranks[1])

        self.assertEqual(1, ranks[2])
        self.assertEqual(1, ranks[3])
        self.assertEqual(1, ranks[4])

        self.assertEqual(2, ranks[5])
        self.assertEqual(2, ranks[6])
        self.assertEqual(2, ranks[7])

        self.assertEqual(3, ranks[8])
        self.assertEqual(3, ranks[9])
        self.assertEqual(3, ranks[10])

        self.assertEqual(4, ranks[11])

    def test_topological_ordering_multidependency_graph(self):
        graph = get_multidependency_graph()
        ranks = calculate_upward_rank(graph)
        self.assertEqual(0, ranks[1])

        self.assertEqual(1, ranks[2])
        self.assertEqual(1, ranks[3])

        self.assertEqual(2, ranks[4])

        self.assertEqual(3, ranks[5])

        self.assertEqual(4, ranks[6])
        self.assertEqual(4, ranks[7])

        self.assertEqual(5, ranks[8])
        self.assertEqual(5, ranks[9])

        self.assertEqual(6, ranks[10])

    def test_topological_sort_no_task(self):
        graph = TaskGraph()
        with self.assertRaises(CreateGraphException):
            sort_topologically(graph)

    def test_topological_sort_single_task(self):
        graph = TaskGraph()
        task = graph.add_new_task(1)
        graph.set_start_task(task.id)
        tasks = sort_topologically(graph)
        self.assertEqual(task.id, tasks[0])

    def test_topological_sort_pipeline_graph(self):
        graph = get_pipeline_graph()
        tasks = sort_topologically(graph)
        self.assertEqual(1, tasks[0])
        self.assertEqual(2, tasks[1])
        self.assertEqual(3, tasks[2])
        self.assertEqual(4, tasks[3])


    def test_topological_sort_paralell_graph(self):
        graph = get_parallel_graph()
        tasks = sort_topologically(graph)
        self.assertEqual(1, tasks[0])
        self.assertEqual(2, tasks[1])
        self.assertEqual(3, tasks[2])
        self.assertEqual(4, tasks[3])
        self.assertEqual(5, tasks[4])
        self.assertEqual(6, tasks[5])
        self.assertEqual(7, tasks[6])

    def test_topological_sort_stencil_graph(self):
        graph = get_stencil_graph()
        tasks = sort_topologically(graph)
        self.assertEqual(1, tasks[0])
        self.assertEqual(2, tasks[1])
        self.assertEqual(3, tasks[2])
        self.assertEqual(4, tasks[3])
        self.assertEqual(5, tasks[4])
        self.assertEqual(6, tasks[5])
        self.assertEqual(7, tasks[6])
        self.assertEqual(8, tasks[7])
        self.assertEqual(9, tasks[8])
        self.assertEqual(10, tasks[9])
        self.assertEqual(11, tasks[10])



    def test_topological_sort_multidependency_graph(self):
        graph = get_multidependency_graph()
        ranks = sort_topologically(graph)
        self.assertEqual(1, ranks[0])
        self.assertEqual(2, ranks[1])
        self.assertEqual(3, ranks[2])
        self.assertEqual(4, ranks[3])
        self.assertEqual(5, ranks[4])
        self.assertEqual(6, ranks[5])
        self.assertEqual(7, ranks[6])
        self.assertEqual(8, ranks[7])
        self.assertEqual(9, ranks[8])
        self.assertEqual(10, ranks[9])
