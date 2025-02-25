import unittest

from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine_boundary import \
    MultiMachineBoundaryCalculator
from src.scheduling.model.machine import Machine
from src.scheduling.model.task_graph import TaskGraph

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

class MultiMachineBoundaryCalculatorTest(unittest.TestCase):

    def test_if_max_predecessors_scheduled_then_lvb_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[5], {3: (5, None)})

        self.assertEqual(65, lcb)
        self.assertEqual(0, lvb)
        self.assertEqual(20, rcb)
        self.assertEqual(16, rvb)

    def test_if_max_predecessors_not_scheduled_then_lvb_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[5], {2: (5, None)})

        self.assertEqual(65, lcb)
        self.assertEqual(10, lvb)
        self.assertEqual(20, rcb)
        self.assertEqual(16, rvb)

    def test_if_lb_is_zero_then_lvb_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[1], {})

        self.assertEqual(0, lcb)
        self.assertEqual(0, lvb)
        self.assertEqual(81, rcb)
        self.assertEqual(27, rvb)

    def test_middle_task_with_no_scheduled_task(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[5], {})

        self.assertEqual(65, lcb)
        self.assertEqual(10, lvb)
        self.assertEqual(20, rcb)
        self.assertEqual(16, rvb)

    def test_if_min_successor_scheduled_then_rvb_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[5], {8: (150, None)})

        self.assertEqual(65, lcb)
        self.assertEqual(10, lvb)
        self.assertEqual(22, rcb)
        self.assertEqual(0, rvb)

    def test_if_min_successor_not_scheduled_then_rvb_must_not_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[5], {6: (153, None)})

        self.assertEqual(65, lcb)
        self.assertEqual(10, lvb)
        self.assertEqual(20, rcb)
        self.assertEqual(16, rvb)

    def test_if_rb_is_zero_then_rvb_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[9], {})

        self.assertEqual(73, lcb)
        self.assertEqual(24, lvb)
        self.assertEqual(0, rcb)
        self.assertEqual(0, rvb)

    def test_if_not_enough_time_then_volatile_boundaries_must_be_zero(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[3], {1: (0, None), 5: (65, None)})

        self.assertEqual(5, lcb)
        self.assertEqual(0, lvb)
        self.assertEqual(107, rcb)
        self.assertEqual(0, rvb)

    def test_if_not_enough_time_then_volatile_boundaries_must_be_zero_2(self):
        calculator, _, graph = self._init_default_calculator(deadline_factor=2)

        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[3], {1: (0, None), 5: (66, None)})

        self.assertEqual(5, lcb)
        self.assertEqual(0, lvb)
        self.assertEqual(106, rcb)
        self.assertEqual(0, rvb)

    def test_if_not_enough_cores_then_volatile_boundaries_must_be_zero(self):
        graph, min_makespan = _get_graph_3()
        machines = [Machine('m1', 1)]

        calculator = MultiMachineBoundaryCalculator(graph, min_makespan * 2, 0.3, machines)
        machines[0].schedule_task(graph.tasks[2], 5)
        machines[0].schedule_task(graph.tasks[4], 75)


        lcb, lvb, rcb, rvb = calculator.calculate_boundaries(graph.tasks[3], {1: (0, None), 5: (125, None)})

        self.assertEqual(5+10, lcb)
        self.assertEqual(0, lvb)
        self.assertEqual(97, rcb)
        self.assertEqual(0, rvb)

    def _init_default_calculator(self, deadline_factor=1):
        graph, min_makespan = _get_graph_3()
        machines = [Machine('m1', 1), Machine('m2', 2)]

        calculator = MultiMachineBoundaryCalculator(graph, min_makespan * deadline_factor, 0.3, machines)

        return calculator, min_makespan, graph