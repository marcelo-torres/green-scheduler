import unittest

from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.task_graph.task import Task
from src.scheduling.task_graph.task_graph import TaskGraph


class EnergyUsageCalculatorTest(unittest.TestCase):

    def test_no_g_power(self):
        green_energy = []
        interval_size = 10
        expected_green_power_available = [(0, 0)]
        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available)

    def test_g_power_available_with_no_task_power(self):
        green_energy = [630, 750, 900, 1050, 1200, 1380, 1200, 2010]
        interval_size = 300
        expected_green_power_available = [(0, 630), (300, 750), (600, 900), (900, 1050), (1200, 1200), (1500, 1380),
                                          (1800, 1200), (2100, 2010), (2400, 0)]
        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available)

    def test_with_task_with_runtime_equal_to_interval_size(self):
        green_energy = [630, 750, 900, 1050, 1200, 1380, 1200, 2010]
        interval_size = 300
        expected_green_power_available = [(0, 630), (300, 750), (600, 900), (900, 1050), (1200, 1200-450), (1500, 1380),
                                          (1800, 1200), (2100, 2010), (2400, 0)]
        scheduled_tasks = [
            (1200, Task(1, runtime=300, power=450))
        ]
        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available, scheduled_tasks=scheduled_tasks)

    def test_with_task_with_runtime_greater_then_interval_size(self):
        green_energy = [630, 750, 900, 1050, 1200, 1380, 1200, 2010]
        interval_size = 300
        expected_green_power_available = [(0, 630), (300, 750), (600, 900), (900, 1050), (1200, 1200 - 450),
                                          (1500, 1380-450), (1501, 1380),
                                          (1800, 1200), (2100, 2010), (2400, 0)]
        scheduled_tasks = [
            (1200, Task(1, runtime=301, power=450))
        ]
        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available,
                                               scheduled_tasks=scheduled_tasks)

    def test_with_two_diff_tasks_at_the_same_time(self):
        green_energy = [630, 750, 900, 1050, 1200, 1380, 1200, 2010]
        interval_size = 300
        expected_green_power_available = [(0, 630), (300, 750), (600, 900), (900, 1050), (1200, 1200-450-70),
                                          (1500, 1380-70), (1501, 1380), (1800, 1200), (2100, 2010), (2400, 0)]
        scheduled_tasks = [
            (1200, Task(1, runtime=301, power=70)),
            (1200, Task(2, runtime=300, power=450))
        ]

        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available, scheduled_tasks=scheduled_tasks)

    def test(self):
        green_energy = [0, 10, 20, 10, 0, 20, 40]
        interval_size = 10

        expected_green_power_available = [
            (0, 0), (22, 10), (30, 0), (50, 10), (51, 20), (60, 40), (70, 0)
        ]

        scheduled_tasks = [
            (10, Task(1, runtime=10, power=10)),
            (20, Task(2, runtime=7, power=10)),
            (20, Task(3, runtime=2, power=10)),
            (27, Task(4, runtime=4, power=10)),
            (31, Task(5, runtime=8, power=10)),
            (31, Task(6, runtime=10, power=10)),
            (50, Task(7, runtime=1, power=10))
        ]

        self.do_test_get_green_power_available(green_energy, interval_size, expected_green_power_available, scheduled_tasks=scheduled_tasks)

    def do_test_get_green_power_available(self, green_energy, interval_size, expected_green_power_available,
                                          scheduled_tasks=None):
        calculator = EnergyUsageCalculator(TaskGraph(), green_energy, interval_size)

        if scheduled_tasks:
            for start_time, task in scheduled_tasks:
                calculator.add_scheduled_task(task, start_time)

        green_power_available = calculator.get_green_power_available()
        self.assertEqual(green_power_available, expected_green_power_available)


