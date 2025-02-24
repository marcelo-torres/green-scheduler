import unittest

from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.calc_levels import calc_levels

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

'''
     1---+
    / \  |
    2 3  |
    \ | /
      4
     / \
     5 6
     \ /
      7
'''


def _get_graph_4():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=10, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=7, power=10)
    graph.add_new_task(3, runtime=2, power=10)
    graph.add_new_task(4, runtime=4, power=10)
    graph.add_new_task(5, runtime=8, power=10)
    graph.add_new_task(6, runtime=9, power=10)
    graph.add_new_task(7, runtime=1, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    graph.create_dependency(2, 4)
    graph.create_dependency(3, 4)

    graph.create_dependency(4, 5)
    graph.create_dependency(4, 6)

    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph, 31


class CalcLevelsTest(unittest.TestCase):

    def test_single_task(self):
        graph, _ = _get_graph_1()

        levels, max_level = calc_levels(graph)

        self.assertEqual(0, max_level)

        self.assertEqual(0, levels[1])

    def test_pipeline(self):
        graph, _ = _get_graph_2()

        levels, max_level = calc_levels(graph)

        self.assertEqual(4, max_level)

        self.assertEqual(0, levels[1])
        self.assertEqual(1, levels[2])
        self.assertEqual(2, levels[3])
        self.assertEqual(3, levels[4])
        self.assertEqual(4, levels[5])

    def test_parallel(self):
        graph, _ = _get_graph_3()

        levels, max_level = calc_levels(graph)

        self.assertEqual(4, max_level)

        self.assertEqual(0, levels[1])
        self.assertEqual(1, levels[2])
        self.assertEqual(1, levels[3])
        self.assertEqual(1, levels[4])
        self.assertEqual(2, levels[5])
        self.assertEqual(3, levels[6])
        self.assertEqual(3, levels[7])
        self.assertEqual(3, levels[8])
        self.assertEqual(4, levels[9])

    def test_taskflow_paper_graph(self):
        graph, _ = _get_graph_4()

        levels, max_level = calc_levels(graph)

        self.assertEqual(4, max_level)

        self.assertEqual(0, levels[1])
        self.assertEqual(1, levels[2])
        self.assertEqual(1, levels[3])
        self.assertEqual(2, levels[4])
        self.assertEqual(3, levels[5])
        self.assertEqual(3, levels[6])
        self.assertEqual(4, levels[7])