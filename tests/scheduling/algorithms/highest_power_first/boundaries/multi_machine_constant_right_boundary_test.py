import unittest

from parameterized import parameterized

from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine_constant_right_boundary import \
    calculate_constant_right_boundary
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

    @parameterized.expand([
        [0, 0],
        [10, 0]
    ])
    def test_single_node(self, deadline_increase, expected_rb):
        graph, min_makespan = _get_graph_1()
        machine = Machine('m1', 1)

        rb, is_limited = calculate_constant_right_boundary(graph.get_first_task(), {}, [machine], min_makespan + deadline_increase)
        self.assertEqual(expected_rb, rb)
        self.assertFalse(is_limited)

    @parameterized.expand([
        [0, 84],
        [10, 84]
    ])
    def test_first_task_of_pipeline(self, deadline_increase, expected_rb):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        rb, is_limited = calculate_constant_right_boundary(graph.get_first_task(), {}, [machine], min_makespan + deadline_increase)

        self.assertEqual(expected_rb, rb)
        self.assertFalse(is_limited)

    @parameterized.expand([
        [0, 0],
        [10, 0]
    ])
    def test_last_task_of_pipeline(self, deadline_increase, expected_rb):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {}, [machine], min_makespan + deadline_increase)

        self.assertEqual(expected_rb, rb)
        self.assertFalse(is_limited)

    @parameterized.expand([
        [0, 9],
        [10, 9]
    ])
    def test_middle_task_of_pipeline(self, deadline_increase, expected_rb):
        graph, min_makespan = _get_graph_2()
        machine = Machine('m1', 1)

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[3], {}, [machine], min_makespan + deadline_increase)

        self.assertEqual(expected_rb, rb)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_with_not_enough_cores(self):
        graph, min_makespan = _get_graph_3()
        machine = Machine('m1', 1)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {}, [machine], min_makespan + deadline_increase)

        self.assertEqual(25, rb)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph(self):
        graph, min_makespan = _get_graph_3()
        machine = Machine('m1', 3)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {}, [machine],
                                                           min_makespan + deadline_increase)

        self.assertEqual(20, rb)
        self.assertFalse(is_limited)


    def test_middle_task_of_graph_two_machines(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 2)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {}, [machine1, machine2],
                                                           min_makespan + deadline_increase)

        self.assertEqual(20, rb)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_multi_machine(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {}, [machine1, machine2, machine3],
                                                           min_makespan + deadline_increase)

        self.assertEqual(20, rb)
        self.assertFalse(is_limited)

    def test_middle_task_of_graph_multi_machine_with_schedule_task(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {8: 150}, [machine1, machine2, machine3],
                                                           min_makespan + deadline_increase)

        self.assertEqual(36, rb)
        self.assertTrue(is_limited)

    def test_middle_task_of_graph_multi_machine_with_other_schedule_task(self):
        graph, min_makespan = _get_graph_3()
        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        deadline_increase = 100

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[5], {7: 166}, [machine1, machine2, machine3],
                                                           min_makespan + deadline_increase)

        self.assertEqual(20, rb)
        self.assertTrue(is_limited)

    def test_bug_rb_length_not_correct(self):
        graph, min_makespan = _get_graph_3()
        machines = [Machine('m1', 1), Machine('m2', 2)]

        rb, is_limited = calculate_constant_right_boundary(graph.tasks[1], {}, machines, min_makespan * 2)

        self.assertEqual(81, rb)
        self.assertFalse(is_limited)
