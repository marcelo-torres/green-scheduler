import unittest

from src.scheduling.algorithms.bounded_boundary_search.boundaries.multi_machine.max_runtime_child import \
    calc_max_runtime_child, sort_by_max_runtime_unschedule
from src.scheduling.model.task_graph import TaskGraph
from tests.scheduling.graph_utils import get_pipeline_graph, get_parallel_graph


def _get_single_task():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=100, power=10)
    graph.set_start_task(task.id)

    return graph


class MaxRuntimeChildTest(unittest.TestCase):

    def test_one_task(self):
        graph = _get_single_task()
        max_runtime = _calc_max_runtime_child(graph.get_first_task(), {}, right_mode=False)
        self.assertEqual(max_runtime, graph.get_first_task().runtime)

    def test_scheduled_predecessor(self):
        graph = get_parallel_graph()
        max_runtime = _calc_max_runtime_child(graph.get_task(7), {2: 1}, right_mode=False)
        self.assertEqual(max_runtime, 7)

    def test_no_scheduled_predecessor_1(self):
        graph = get_parallel_graph()
        max_runtime = _calc_max_runtime_child(graph.get_task(7), {}, right_mode=False)
        self.assertEqual(max_runtime, 10)

    def test_no_scheduled_predecessor_2(self):
        graph = get_parallel_graph()
        max_runtime = _calc_max_runtime_child(graph.get_task(5), {}, right_mode=False)
        self.assertEqual(max_runtime, 7)

    def test_sort_predecessors(self):
        graph = get_parallel_graph()
        tasks = _sort_by_max_runtime_unschedule(graph.get_task(7).predecessors, {}, right_mode=False)
        self.assertEqual(tasks[0].id, 4)
        self.assertEqual(tasks[1].id, 6)
        self.assertEqual(tasks[2].id, 5)

    def test_sort_predecessors_2(self):
        graph = get_parallel_graph()
        tasks = _sort_by_max_runtime_unschedule(graph.get_task(7).predecessors, {3: 1, 2: 4, 1: 1}, right_mode=False)
        self.assertEqual(tasks[0].id, 6)
        self.assertEqual(tasks[1].id, 4)
        self.assertEqual(tasks[2].id, 5)

    def test_sort_predecessors_3(self):
        graph = get_parallel_graph()
        tasks = _sort_by_max_runtime_unschedule(graph.get_task(7).predecessors, {3: 1, 2: 4, 1: 1, 6: 0}, right_mode=False)
        self.assertEqual(tasks[0].id, 4)
        self.assertEqual(tasks[1].id, 5)
        self.assertEqual(tasks[2].id, 6)

    def test_sort_successors_1(self):
        graph = get_parallel_graph()
        tasks = _sort_by_max_runtime_unschedule(graph.get_task(1).successors, {}, right_mode=True)
        self.assertEqual(tasks[0].id, 2)
        self.assertEqual(tasks[1].id, 3)

    def test_sort_successors_2(self):
        graph = get_parallel_graph()
        tasks = _sort_by_max_runtime_unschedule(graph.get_task(1).successors, {2: 2}, right_mode=True)
        self.assertEqual(tasks[0].id, 3)
        self.assertEqual(tasks[1].id, 2)

    def test_one_task_right_mode(self):
        graph = _get_single_task()
        max_runtime = _calc_max_runtime_child(graph.get_first_task(), {}, right_mode=True)
        self.assertEqual(max_runtime, graph.get_first_task().runtime)

    def test_schedule_successor_task(self):
        pass


def _calc_max_runtime_child(task, schedule, right_mode):
    def is_scheduled_task(task_id):
        return task_id in schedule

    return calc_max_runtime_child(task, is_scheduled_task, right_mode)

def _sort_by_max_runtime_unschedule(tasks, schedule, right_mode=False):

    def is_scheduled_task(task_id):
        return task_id in schedule

    return sort_by_max_runtime_unschedule(tasks, is_scheduled_task, right_mode=right_mode)