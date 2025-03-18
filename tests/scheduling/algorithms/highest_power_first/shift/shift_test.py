import unittest

from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_boundary import \
    MultiMachineBoundaryCalculator
from src.scheduling.algorithms.highest_power_first.shift_left.shift import shift_tasks_to_save_energy
from src.scheduling.algorithms.highest_power_first.shift_left.shift_greedy import \
    shift_tasks_to_save_energy_greedy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import _create_machine_map
from src.scheduling.model.machine import Machine
from src.scheduling.model.task_graph import TaskGraph
from tests.scheduling.graph_utils import get_pipeline_graph

def _get_graph_1():
    """
          1
          |
          +
         /|\
        / | \
       2  3 4
       | |  |
       \ | /
        \|/
         +
         |
         5

    :return: TaskGraph
    """

    graph = TaskGraph()
    task_1 = graph.add_new_task(1, 1, 1)

    task_2 = graph.add_new_task(2, 2, 2)
    task_3 = graph.add_new_task(3, 3, 3)
    task_4 = graph.add_new_task(4, 4, 4)

    task_5 = graph.add_new_task(5, 5, 5)

    graph.set_start_task(task_1.id)

    graph.create_dependency(task_1.id, task_2.id)
    graph.create_dependency(task_1.id, task_3.id)
    graph.create_dependency(task_1.id, task_4.id)

    graph.create_dependency(task_2.id, task_5.id)
    graph.create_dependency(task_3.id, task_5.id)
    graph.create_dependency(task_4.id, task_5.id)

    return graph


def _get_default_config_for_multi_machine(graph, deadline, schedule, green_power=[100] * 10, interval_size=10,
                                          machines=None, boundary_calc=None, energy_usage_calc=None):

    if machines is None:
        machines = [
            Machine('m1', 1, 0),
            Machine('m2', 1, 0)
        ]
    machines_map = _create_machine_map(machines)

    if boundary_calc is None:
        boundary_calc = MultiMachineBoundaryCalculator(graph, deadline, 0.5, machines)

    if energy_usage_calc is None:
        energy_usage_calc = EnergyUsageCalculator(green_power, interval_size)

    for task_id, s in schedule.items():
        start_time, machine_id = s
        machine = machines_map[machine_id]
        task = graph.get_task(task_id)
        energy_usage_calc.add_scheduled_task(task, start_time)
        machine.schedule_task(task, start_time)

    return machines, boundary_calc, energy_usage_calc


class ShiftLeftTest(unittest.TestCase):

    def test_schedule_pipeline_shift_left(self):
        for shift_left_function in ['default', 'greedy']:
            with self.subTest(msg=shift_left_function):
                green_power = [10, 0, 10, 5, 0, 8, 6, 7]
                graph = get_pipeline_graph()

                deadline = 80
                machines = [
                    Machine('m1', 100, 0)
                ]

                boundary_calc = MultiMachineBoundaryCalculator(graph, deadline, 0.5, machines)
                energy_usage_calc = EnergyUsageCalculator(green_power, 10)

                scheduling = {
                    1: (2, 'm1'),
                    2: (21, 'm1'),
                    3: (64, 'm1'),
                    4: (71, 'm1')
                }


                for task_id, s in scheduling.items():
                    start_time, machine = s
                    task = graph.get_task(task_id)
                    machines[0].schedule_task(task, start_time)
                    energy_usage_calc.add_scheduled_task(task, start_time)

                if shift_left_function == 'default':
                    shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calc)
                elif shift_left_function == 'greedy':
                    shift_tasks_to_save_energy_greedy(graph, scheduling, boundary_calc, energy_usage_calc)
                else:
                    raise Exception(f'{shift_left_function} not defined')

                self.assertEqual(scheduling[1][0], 0)
                self.assertEqual(scheduling[2][0], 20)
                self.assertEqual(scheduling[3][0], 30)
                self.assertEqual(scheduling[4][0], 50)

    def test_schedule_pipeline_shift_right(self):

        green_power = [0, 5, 0, 5, 10]
        deadline = 80

        graph = TaskGraph()
        graph.add_new_task(1, 5, 4)
        graph.add_new_task(2, 5, 6)
        graph.create_dependency(1, 2)
        graph.set_start_task(1)

        machines = [
            Machine('m1', 100, 0)
        ]

        boundary_calc = MultiMachineBoundaryCalculator(graph, deadline, 0.5, machines)
        energy_usage_calc = EnergyUsageCalculator(green_power, 10)

        scheduling = {
            1: (0, 'm1'),
            2: (10, 'm1'),
        }

        for task_id, s in scheduling.items():
            start_time, machine = s
            task = graph.get_task(task_id)
            machines[0].schedule_task(task, start_time)
            energy_usage_calc.add_scheduled_task(task, start_time)

        shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertEqual(40, scheduling[1][0])
        self.assertEqual(45, scheduling[2][0])

    def test_schedule_graph_shift_left_multi_machine_green_no_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (0, 'm1'),
            2: (1, 'm1'),
            3: (3, 'm1'),
            4: (6, 'm1'),
            5: (10, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule)
        machines[1].state.use_cores(0, 10, 1)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 1, 'm1')
        self.assertTaskIn(schedule, 3, 3, 'm1')
        self.assertTaskIn(schedule, 4, 6, 'm1')
        self.assertTaskIn(schedule, 5, 10, 'm1')

    def test_schedule_graph_shift_left_multi_machine_no_green_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (0, 'm1'),
            2: (1, 'm1'),
            3: (3, 'm1'),
            4: (6, 'm1'),
            5: (10, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule, green_power=[0]*10)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 1, 'm1')
        self.assertTaskIn(schedule, 3, 1, 'm2')
        self.assertTaskIn(schedule, 4, 3, 'm1')
        self.assertTaskIn(schedule, 5, 7, 'm1')

    def test_schedule_graph_shift_left_multi_machine_green_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (0, 'm1'),
            2: (1, 'm1'),
            3: (3, 'm1'),
            4: (6, 'm1'),
            5: (10, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 1, 'm1')
        self.assertTaskIn(schedule, 3, 1, 'm2')
        self.assertTaskIn(schedule, 4, 3, 'm1')
        self.assertTaskIn(schedule, 5, 7, 'm1')

    def test_schedule_graph_shift_left_multi_machine_no_green_no_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (0, 'm1'),
            2: (1, 'm1'),
            3: (3, 'm1'),
            4: (6, 'm1'),
            5: (10, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule,
                                                                                           green_power=[0] * 10)
        machines[1].state.use_cores(0, 10, 1)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 1, 'm1')
        self.assertTaskIn(schedule, 3, 3, 'm1')
        self.assertTaskIn(schedule, 4, 6, 'm1')
        self.assertTaskIn(schedule, 5, 10, 'm1')

    def test_schedule_graph_right_multi_machine_green_no_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (85, 'm1'),
            2: (86, 'm1'),
            3: (88, 'm1'),
            4: (91, 'm1'),
            5: (95, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule)
        machines[1].state.use_cores(85, 15, 1)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertTaskIn(schedule, 1, 85, 'm1')
        self.assertTaskIn(schedule, 2, 86, 'm1')
        self.assertTaskIn(schedule, 3, 88, 'm1')
        self.assertTaskIn(schedule, 4, 91, 'm1')
        self.assertTaskIn(schedule, 5, 95, 'm1')

    def test_schedule_graph_right_multi_machine_no_green_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (85, 'm1'),
            2: (86, 'm1'),
            3: (88, 'm1'),
            4: (91, 'm1'),
            5: (95, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule, green_power=[0]*10)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertTaskIn(schedule, 1, 89, 'm1')
        self.assertTaskIn(schedule, 2, 90, 'm2')
        self.assertTaskIn(schedule, 3, 92, 'm2')
        self.assertTaskIn(schedule, 4, 91, 'm1')
        self.assertTaskIn(schedule, 5, 95, 'm1')

    def test_schedule_graph_right_multi_machine_green_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (85, 'm1'),
            2: (86, 'm1'),
            3: (88, 'm1'),
            4: (91, 'm1'),
            5: (95, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertTaskIn(schedule, 1, 89, 'm1')
        self.assertTaskIn(schedule, 2, 90, 'm2')
        self.assertTaskIn(schedule, 3, 92, 'm2')
        self.assertTaskIn(schedule, 4, 91, 'm1')
        self.assertTaskIn(schedule, 5, 95, 'm1')

    def test_schedule_graph_right_multi_machine_no_green_no_cores(self):
        graph = _get_graph_1()
        deadline = 100
        schedule = {
            1: (85, 'm1'),
            2: (86, 'm1'),
            3: (88, 'm1'),
            4: (91, 'm1'),
            5: (95, 'm1'),
        }

        machines, boundary_calc, energy_usage_calc = _get_default_config_for_multi_machine(graph, deadline, schedule,
                                                                                           green_power=[0] * 10)
        machines[1].state.use_cores(85, 15, 1)

        shift_tasks_to_save_energy(graph, schedule, machines, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertTaskIn(schedule, 1, 85, 'm1')
        self.assertTaskIn(schedule, 2, 86, 'm1')
        self.assertTaskIn(schedule, 3, 88, 'm1')
        self.assertTaskIn(schedule, 4, 91, 'm1')
        self.assertTaskIn(schedule, 5, 95, 'm1')

    def assertTaskIn(self, schedule, task_id, expected_start_time, expected_machine_id):
        self.assertEqual(expected_start_time, schedule[task_id][0])
        self.assertEqual(expected_machine_id, schedule[task_id][1])
