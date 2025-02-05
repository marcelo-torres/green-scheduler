import unittest

from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.makespan_calculator import calc_makespan


class TopologicalOrderingTest(unittest.TestCase):

    def test_makespan_of_single_task_beggining(self):
        graph = TaskGraph()

        task = graph.add_new_task(1, 956, 10)

        scheduling = {
            task.id: 0
        }

        makespan = calc_makespan(scheduling, graph)
        self.assertEqual(task.runtime, makespan)

    def test_makespan_of_single_task_end(self):
        graph = TaskGraph()

        task = graph.add_new_task(1, 956, 10)

        scheduling = {
            task.id: 100
        }

        makespan = calc_makespan(scheduling, graph)
        self.assertEqual(task.runtime + scheduling[task.id], makespan)

    def test_makespan_of_tasks(self):
        graph = TaskGraph()

        task_1 = graph.add_new_task(1, 956, 10)
        task_2 = graph.add_new_task(2, 23, 10)
        task_3 = graph.add_new_task(3, 1000, 10)
        task_4 = graph.add_new_task(4, 12, 10)

        scheduling = {
            task_1.id: 45,
            task_2.id: 3,
            task_3.id: 0,
            task_4.id: 90,
        }

        makespan = calc_makespan(scheduling, graph)
        self.assertEqual(task_1.runtime + scheduling[task_1.id], makespan)