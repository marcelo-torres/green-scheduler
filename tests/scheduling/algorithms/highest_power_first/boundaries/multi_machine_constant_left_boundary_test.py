import unittest

from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine_constant_left_boundary import \
    calculate_constant_left_boundary
from src.scheduling.model.machine import Machine
from src.scheduling.model.task_graph import TaskGraph



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

class MultiMachineConstantLeftBoundaryTest(unittest.TestCase):

    def test_single_node(self):
        graph, min_makespan = _get_graph_1()
        machine = Machine('m1', 1)

        start, is_limited = calculate_constant_left_boundary(graph.get_first_task(), {}, [machine])

        self.assertEqual(0, start)
        self.assertFalse(is_limited)

    def test_first_task_of_pipeline(self):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        start, is_limited = calculate_constant_left_boundary(graph.get_first_task(), {}, [machine])

        self.assertEqual(0, start)
        self.assertFalse(is_limited)

    def test_last_task_of_pipeline(self):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {}, [machine])

        self.assertEqual(79, start)
        self.assertFalse(is_limited)

    def test_middle_task_of_pipeline(self):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[3], {}, [machine])

        self.assertEqual(73, start)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_with_not_enough_cores(self):
        graph, min_makespan = _get_graph_3()
        machine = Machine('m1', 1)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {}, [machine])

        self.assertEqual(125, start)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph(self):
        graph, min_makespan = _get_graph_3()
        machine = Machine('m1', 3)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {}, [machine])

        self.assertEqual(65, start)
        self.assertFalse(is_limited)


    def test_middle_task_of_graph_two_machines(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 2)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {}, [machine1, machine2])

        self.assertEqual(65, start)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_multi_machine(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {}, [machine1, machine2, machine3])

        self.assertEqual(65, start)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_multi_machine_with_schedule_task(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        start, is_limited = calculate_constant_left_boundary(graph.tasks[5], {1: (15, None)}, [machine1, machine2, machine3])

        self.assertEqual(65 + 15, start)
        self.assertFalse(is_limited)