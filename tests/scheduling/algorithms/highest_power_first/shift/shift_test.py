import unittest

from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.algorithms.highest_power_first.shift_left.shift import shift_tasks_to_save_energy
from src.scheduling.algorithms.highest_power_first.shift_left.shift_greedy import \
    shift_tasks_to_save_energy_greedy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.task_graph import TaskGraph
from tests.scheduling.graph_utils import get_pipeline_graph


class ShiftLeftTest(unittest.TestCase):

    def test_schedule_pipeline_shift_left(self):
        for shift_left_function in ['default', 'greedy']:
            with self.subTest(msg=shift_left_function):
                green_power = [10, 0, 10, 5, 0, 8, 6, 7]
                graph = get_pipeline_graph()

                deadline = 80

                boundary_calc = BoundaryCalculator(graph, deadline, 0.5)
                energy_usage_calc = EnergyUsageCalculator(green_power, 10)

                scheduling = {
                    1: 2,
                    2: 21,
                    3: 64,
                    4: 71
                }

                for task_id, start_time in scheduling.items():
                    task = graph.get_task(task_id)
                    energy_usage_calc.add_scheduled_task(task, start_time)

                if shift_left_function == 'default':
                    shift_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calc)
                elif shift_left_function == 'greedy':
                    shift_tasks_to_save_energy_greedy(graph, scheduling, boundary_calc, energy_usage_calc)
                else:
                    raise Exception(f'{shift_left_function} not defined')

                self.assertEqual(scheduling[1], 0)
                self.assertEqual(scheduling[2], 20)
                self.assertEqual(scheduling[3], 30)
                self.assertEqual(scheduling[4], 50)

    def test_schedule_pipeline_shift_right(self):

        green_power = [0, 5, 0, 5, 10]
        deadline = 80

        graph = TaskGraph()
        graph.add_new_task(1, 5, 4)
        graph.add_new_task(2, 5, 6)
        graph.create_dependency(1, 2)
        graph.set_start_task(1)

        boundary_calc = BoundaryCalculator(graph, deadline, 0.5)
        energy_usage_calc = EnergyUsageCalculator(green_power, 10)

        scheduling = {
            1: 0,
            2: 10,
        }

        for task_id, start_time in scheduling.items():
            task = graph.get_task(task_id)
            energy_usage_calc.add_scheduled_task(task, start_time)

        shift_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calc, mode='right')

        self.assertEqual(40, scheduling[1])
        self.assertEqual(45, scheduling[2])
