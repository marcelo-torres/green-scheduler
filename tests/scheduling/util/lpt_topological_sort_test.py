import unittest

from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort


class LptTopologicalSortTest(unittest.TestCase):

    def test_get_topological_list_single_task(self):
        graph = _get_single_task_graph()
        lpt_sort = LtpTopologicalSort(graph)
        tasks = list(lpt_sort.get_lpt_topological_list())

        self.assertEqual(1, len(tasks))
        self.assertEqual(graph.get_first_task().id, tasks[0].id)

    def test_get_topological_list_until_single_task(self):
        graph = _get_single_task_graph()
        lpt_sort = LtpTopologicalSort(graph)
        tasks = list(lpt_sort.lpt_topological_list_until(graph.get_first_task()))

        self.assertEqual(0, len(tasks))

    def test_get_topological_list_until_first_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        tasks = list(lpt_sort.lpt_topological_list_until(graph.get_first_task()))

        self.assertEqual(0, len(tasks))

    def test_get_topological_list_until_middle_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        task = graph.get_task(7)
        tasks = list(lpt_sort.lpt_topological_list_until(task))

        self.assertEqual(5, len(tasks))

        self.assertEqual(1, tasks[0].id)

        self.assertEqual(3, tasks[1].id)
        self.assertEqual(4, tasks[2].id)
        self.assertEqual(2, tasks[3].id)

        self.assertEqual(5, tasks[4].id)

    def test_get_topological_list_until_last_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        task = graph.get_task(9)
        tasks = list(lpt_sort.lpt_topological_list_until(task))

        self.assertEqual(8, len(tasks))

        self.assertEqual(1, tasks[0].id)

        self.assertEqual(3, tasks[1].id)
        self.assertEqual(4, tasks[2].id)
        self.assertEqual(2, tasks[3].id)

        self.assertEqual(5, tasks[4].id)

        self.assertEqual(8, tasks[5].id)
        self.assertEqual(7, tasks[6].id)
        self.assertEqual(6, tasks[7].id)

    def test_lpt_topological_inverse_list_until_single_task(self):
        graph = _get_single_task_graph()
        lpt_sort = LtpTopologicalSort(graph)
        tasks = list(lpt_sort.lpt_topological_inverse_list_until(graph.get_first_task()))

        self.assertEqual(0, len(tasks))

    def test_lpt_topological_inverse_list_until_first_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        tasks = list(lpt_sort.lpt_topological_inverse_list_until(graph.get_first_task()))

        self.assertEqual(8, len(tasks))

        self.assertEqual(9, tasks[0].id)

        self.assertEqual(8, tasks[1].id)
        self.assertEqual(7, tasks[2].id)
        self.assertEqual(6, tasks[3].id)

        self.assertEqual(5, tasks[4].id)

        self.assertEqual(3, tasks[5].id)
        self.assertEqual(4, tasks[6].id)
        self.assertEqual(2, tasks[7].id)

    def test_lpt_topological_inverse_list_until_middle_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        task = graph.get_task(3)
        tasks = list(lpt_sort.lpt_topological_inverse_list_until(task))

        self.assertEqual(5, len(tasks))

        self.assertEqual(9, tasks[0].id)

        self.assertEqual(8, tasks[1].id)
        self.assertEqual(7, tasks[2].id)
        self.assertEqual(6, tasks[3].id)

        self.assertEqual(5, tasks[4].id)

    def test_lpt_topological_inverse_list_until_last_task(self):
        graph = _get_graph()
        lpt_sort = LtpTopologicalSort(graph)
        task = graph.get_task(9)
        tasks = list(lpt_sort.lpt_topological_inverse_list_until(task))

        self.assertEqual(0, len(tasks))


def _get_single_task_graph():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=5, power=10)
    graph.set_start_task(task.id)

    return graph


'''
        +-> 2 -+             +-> 6 -+
       /         \          /        \
    1-+---> 3 ----+--> 5 --+---> 7 ---+--> 9
       \         /          \        /
        +-> 4 -+             +-> 8 -+
'''
def _get_graph():
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

    return graph