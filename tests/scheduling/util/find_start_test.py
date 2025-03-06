import unittest

from parameterized import parameterized

from src.scheduling.model.machine import Machine
from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.find_start import find_min_start_machine, find_max_start_machine

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

    return graph, 86


class FindStartTest(unittest.TestCase):

    def test_find_min_should_iterate_until_end(self):
        graph, _ = _get_graph()
        task = graph.tasks[4]

        machine = Machine('m1', 1)
        machine.schedule_task(graph.tasks[1], 0)
        machine.schedule_task(graph.tasks[2], 5)
        machine.schedule_task(graph.tasks[3], 15)

        min_start, min_machine = find_min_start_machine(task, [machine], 5)

        self.assertEqual(75, min_start)
        self.assertEqual('m1', min_machine.id)

    def test_find_min_should_find_start(self):
        graph, _ = _get_graph()
        task = graph.tasks[1]

        machine = Machine('m1', 1)

        min_start, min_machine = find_min_start_machine(task, [machine], 0)

        self.assertEqual(0, min_start)
        self.assertEqual('m1', min_machine.id)

    def test_find_min_should_find_multi_machine(self):
        graph, _ = _get_graph()
        task = graph.tasks[4]

        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)
        machine3 = Machine('m3', 1)

        machine1.schedule_task(graph.tasks[1], 0)
        machine1.schedule_task(graph.tasks[2], 5)
        machine3.schedule_task(graph.tasks[3], 5)

        min_start, min_machine = find_min_start_machine(task, [machine1, machine2, machine3], 5)

        self.assertEqual(5, min_start)
        self.assertEqual('m2', min_machine.id)

    def test_find_min_full_machine(self):
        graph, _ = _get_graph()
        task = graph.tasks[1]

        machine = Machine('m1', 1)
        machine.schedule_task(graph.tasks[2], 0)
        machine.schedule_task(graph.tasks[3], 15)

        min_start, min_machine = find_min_start_machine(task, [machine], 0, end_limit=0)

        self.assertEqual(float('inf'), min_start) # Schedule violation
        self.assertIsNone(min_machine)

    def test_find_min_full_machine_2(self):
        graph, _ = _get_graph()
        task = graph.tasks[1]

        machine1 = Machine('m1', 1)
        machine2 = Machine('m1', 1)
        machine1.schedule_task(graph.tasks[2], 0)
        machine1.schedule_task(graph.tasks[3], 10)

        min_start, min_machine = find_min_start_machine(task, [machine1, machine2], 0, end_limit=0)

        self.assertEqual(float('inf'), min_start)
        self.assertIsNone(min_machine)

    def test_find_max_should_iterate_until_start(self):
        graph, min_makespan = _get_graph()
        task = graph.tasks[1]

        machine = Machine('m1', 1)

        machine.schedule_task(graph.tasks[2], 5)
        machine.schedule_task(graph.tasks[3], 15)
        machine.schedule_task(graph.tasks[4], 75)
        machine.schedule_task(graph.tasks[5], 126)

        max_start, max_machine = find_max_start_machine(task, [machine], 5)

        self.assertEqual(0, max_start)
        self.assertEqual('m1', max_machine.id)

    def test_find_max_should_iterate_until_shifted_start(self):
        graph, min_makespan = _get_graph()
        task = graph.tasks[2]

        machine = Machine('m1', 1)

        machine.schedule_task(graph.tasks[3], 25)
        machine.schedule_task(graph.tasks[4], 85)
        machine.schedule_task(graph.tasks[5], 136)

        max_start, max_machine = find_max_start_machine(task, [machine], 136)

        self.assertEqual(15, max_start)
        self.assertEqual('m1', max_machine.id)

    def test_find_max_should_find_first_interval(self):
        graph, min_makespan = _get_graph()
        task = graph.tasks[1]

        deadline = 1000
        max_successor_start_time = deadline - min_makespan + task.runtime

        machine = Machine('m1', 1)

        max_start, max_machine = find_max_start_machine(task, [machine], max_successor_start_time)

        self.assertEqual(deadline - min_makespan, max_start)
        self.assertEqual('m1', max_machine.id)

    def test_find_max_should_stop_in_limit(self):
        graph, min_makespan = _get_graph()
        task = graph.tasks[1]

        machine = Machine('m1', 1)

        machine.schedule_task(graph.tasks[2], 5)
        machine.schedule_task(graph.tasks[3], 15)
        machine.schedule_task(graph.tasks[4], 75)
        machine.schedule_task(graph.tasks[5], 126)

        max_start, max_machine = find_max_start_machine(task, [machine], 5, start_limit=75)

        self.assertEqual(float('-inf'), max_start)
        self.assertIsNone(max_machine)

    def test_bug_find_max_check_start_limit_first(self):
        graph = TaskGraph()
        task_1 = graph.add_new_task(1, 1, 1)
        task_2 = graph.add_new_task(2, 2, 2)
        task_3 = graph.add_new_task(3, 3, 3)

        graph.set_start_task(task_1.id)

        graph.create_dependency(task_1.id, task_2.id)
        graph.create_dependency(task_2.id, task_3.id)

        machine1 = Machine('m1', 1)
        machine2 = Machine('m2', 1)

        machine2.state.use_cores(0, 100, 1)

        max_start, max_machine = find_max_start_machine(task_1, [machine1, machine2], 1, start_limit=0)

        self.assertEqual(0, max_start)

