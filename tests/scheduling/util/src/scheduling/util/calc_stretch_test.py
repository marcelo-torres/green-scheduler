import unittest

from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.stretch_calculator import calc_stretch


def get_graph():
    graph = TaskGraph()

    task_0 = graph.add_new_task(0, 0, 0)
    task_1 = graph.add_new_task(1, 956, 10)
    task_2 = graph.add_new_task(2, 23, 10)
    task_3 = graph.add_new_task(3, 1000, 10)
    task_4 = graph.add_new_task(4, 12, 10)

    graph.set_start_task(task_0.id)

    graph.create_dependency(task_0.id, task_1.id)
    graph.create_dependency(task_0.id, task_2.id)
    graph.create_dependency(task_0.id, task_3.id)
    graph.create_dependency(task_0.id, task_4.id)

    scheduling = {
        task_0.id: 0,
        task_1.id: 45,
        task_2.id: 3,
        task_3.id: 47,
        task_4.id: 90,
    }

    return graph, scheduling


class CalcStretchTest(unittest.TestCase):

    def test_must_remove_fake_task(self):
        graph, scheduling = get_graph()

        workflow_stretch = calc_stretch(graph, scheduling, makespan=1047, ignore_fake_task=True)
        self.assertEqual(1047 - 3, workflow_stretch)

    def test_must_calculate_makespan_if_it_is_none(self):
        graph, scheduling = get_graph()

        workflow_stretch = calc_stretch(graph, scheduling, makespan=1047, ignore_fake_task=False)
        self.assertEqual(1047 - 0, workflow_stretch)

    def test_must_calculate_strench(self):
        graph, scheduling = get_graph()

        workflow_stretch = calc_stretch(graph, scheduling, makespan=None, ignore_fake_task=True)
        self.assertEqual(1047 - 3, workflow_stretch)
