import unittest

from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.task_graph.task_graph import TaskGraph

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


def _get_graph_1():
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

    return graph, 78


'''
        +-> 2
       /   
    1-+---> 3
       \     
        +-> 4
'''
def _get_graph_3():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=5, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=10, power=10)
    graph.add_new_task(3, runtime=60, power=10)
    graph.add_new_task(4, runtime=50, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    return graph, 65


class HighestPowerFirstTest(unittest.TestCase):

    def test_single_task_with_no_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 4, [], 0)

        self.assertEqual(0, scheduling[task.id])

    def test_single_task_with_no_g_energy_and_no_slack(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 2, [], 0)

        self.assertEqual(0, scheduling[task.id])

    def test_single_task_with_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 4, [0, 10, 0, 10], 40)

        self.assertEqual(40, scheduling[task.id])

    def test_single_task_with_increasing_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 2, [0, 10, 20, 30, 40, 50, 60], 20)

        self.assertEqual(20, scheduling[task.id])

    def test_single_task_with_decreasing_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 2, [60, 50, 40, 30, 20, 10, 0], 20)

        self.assertEqual(0, scheduling[task.id])

    def test_single_task_with_variable_g_energy_1(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 4, [0, 10, 20, 10, 0, 0, 0, 5, 10, 5, 0, 0, 0, 50, 20, 10, 0],
                                    20)

        self.assertEqual(0, scheduling[task.id])

    def test_single_task_with_variable_g_energy_2(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        scheduling = schedule_graph(graph, task.runtime * 4, [0, 10, 20, 10, 0, 0, 0, 5, 10, 5, 0, 0, 0, 50, 20, 10, 1],
                                    20)

        self.assertEqual(240, scheduling[task.id])

    def test_multiple_tasks_g1_with_no_g_energy(self):
        graph, min_makespan = _get_graph_1()

        scheduling = schedule_graph(graph, min_makespan * 4, [], 0)

        self.assertEqual(0, scheduling[1])
        self.assertEqual(10, scheduling[2])
        self.assertEqual(10, scheduling[3])
        self.assertEqual(17, scheduling[4])
        self.assertEqual(21, scheduling[5])
        self.assertEqual(21, scheduling[6])
        self.assertEqual(30, scheduling[7])

    def test_multiple_tasks_g2_with_no_g_energy(self):
        graph, min_makespan = _get_graph_2()

        scheduling = schedule_graph(graph, min_makespan * 4, [], 0)

        self.assertEqual(0, scheduling[1])
        self.assertEqual(3, scheduling[2])
        self.assertEqual(73, scheduling[3])
        self.assertEqual(78, scheduling[4])
        self.assertEqual(79, scheduling[5])

    def test_multiple_tasks_g1_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        scheduling = schedule_graph(graph, min_makespan * 2, [0, 10, 20, 20, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        self.assertEqual(10, scheduling[1])
        self.assertEqual(20, scheduling[2])
        self.assertEqual(20, scheduling[3])
        self.assertEqual(27, scheduling[4])
        self.assertEqual(31, scheduling[5])
        self.assertEqual(31, scheduling[6])
        self.assertEqual(50, scheduling[7])

    def test_multiple_tasks_g1_with_no_slack_to_delay_and_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        scheduling = schedule_graph(graph, min_makespan * 2, [0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        self.assertEqual(10, scheduling[1])
        self.assertEqual(20, scheduling[2])
        self.assertEqual(20, scheduling[3])
        self.assertEqual(27, scheduling[4])
        self.assertEqual(31, scheduling[5])
        self.assertEqual(47, scheduling[6])
        self.assertEqual(57, scheduling[7])

    def test_multiple_tasks_g1_with_no_slack_to_delay_and_with_g_energy_and_right_left(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        scheduling = schedule_graph(graph, min_makespan * 2, [0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10, shift_mode='right-left')

        self.assertEqual(10, scheduling[1])
        self.assertEqual(20, scheduling[2])
        self.assertEqual(20, scheduling[3])
        self.assertEqual(27, scheduling[4])
        self.assertEqual(31, scheduling[5])
        self.assertEqual(50, scheduling[6])
        self.assertEqual(60, scheduling[7])

    def test_multiple_tasks_g1_with_slack_to_delay_and_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        scheduling = schedule_graph(graph, min_makespan * 4, [0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        self.assertEqual(10, scheduling[1])
        self.assertEqual(20, scheduling[2])
        self.assertEqual(20, scheduling[3])
        self.assertEqual(27, scheduling[4])
        self.assertEqual(31, scheduling[5])
        self.assertEqual(50, scheduling[6])
        self.assertEqual(60, scheduling[7])

    def test_runtime_ascending(self):
        graph, min_makespan = _get_graph_3()

        scheduling = schedule_graph(graph, min_makespan * 4, [10, 10], 100, task_ordering='runtime_ascending')
        scheduling = list(scheduling.items())
        scheduling.sort(key=lambda schedule: schedule[1])

        tasks = list(
            map(lambda schedule: schedule[0], scheduling)
        )

        self.assertEqual(1, tasks[0])
        self.assertEqual(2, tasks[1])
        self.assertEqual(4, tasks[2])
        self.assertEqual(3, tasks[3])



