import pytest

from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy import _slice_green_power_available_list, \
    _find_min_brown_energy_in_interval, _calculate_brown_energy_of_task, find_min_brown_energy, IntervalException
from src.scheduling.energy.find_min_brown_energy_greedy import find_min_brown_energy_greedy
from src.scheduling.task_graph.task import Task
from src.scheduling.task_graph.task_graph import TaskGraph


def test_task_power_equal_to_zero_():
    lb = 100
    task = Task(1, 5, 0)
    start_min = find_min_brown_energy(task, lb, 50, 458, [])
    assert lb == start_min


def test_interval_length_not_enough():
    task = Task(1, 51, 10)
    try:
        find_min_brown_energy(task, 25, 25, 100, [(0, 5), (50, 100)])
        assert False
    except IntervalException:
        assert True


def test_interval_length_enough():
    task = Task(1, 50, 10)
    start_min = find_min_brown_energy(task, 25, 25, 100, [(0, 5), (50, 100)])
    assert start_min == 25


def test_with_green_energy_available_is_null():
    task = Task(1, 50, 10)
    start_min = find_min_brown_energy(task, 10, 8, 100, [])
    assert start_min == 10

@pytest.mark.parametrize(
    'expected_start_min, lb, rb, deadline, green_power, green_interval_size, task, scheduled_tasks',
    [
        (0, 0, 0, 50, [0, 0, 0, 0, 0, 0], 10, Task(1, 17, 100), []),
        (13, 0, 0, 100, [1, 4, 5, 0, 10, 0], 5, Task(1, 12, 9), []),
    ]
)
def test_find_min_brown_energy_equal_to_greedy(expected_start_min, lb, rb, deadline, green_power, green_interval_size, task, scheduled_tasks):
    graph = TaskGraph()
    graph.add_task(task)

    energy_usage_calculator = EnergyUsageCalculator(green_power, green_interval_size)

    for start_time, scheduled_task in scheduled_tasks:
        graph.add_task(scheduled_task)
        energy_usage_calculator.add_scheduled_task(scheduled_task, start_time)

    start_min = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calculator.get_green_power_available())
    start_min_greedy = find_min_brown_energy_greedy(task, lb, rb, deadline, energy_usage_calculator)

    assert expected_start_min == start_min
    assert start_min == start_min_greedy




@pytest.mark.parametrize(
    "task_runtime, task_power, task_start, current_green_events, expected_brown_energy",
    [
        (10, 100, 0, [(0, 100), (10, 200), (15, 0), (20, 130)], 0),
        (5, 200, 10, [(0, 100), (10, 200), (15, 0), (20, 130)], 0),
        (5, 199, 10, [(0, 100), (10, 200), (15, 0), (20, 130)], 0),
        (15, 200, 0, [(0, 100), (10, 200), (15, 0), (20, 130)], 1000),
        (20, 100, 0, [(0, 100), (10, 200), (15, 0), (20, 130)], 500),
        (20, 200, 0, [(0, 100), (10, 200), (15, 0), (20, 130)], 2000),
        (3, 200, 17, [(15, 0), (20, 130)], 600)
     ]
)
def test_calculate_brown_energy_of_task(task_runtime, task_power, task_start, current_green_events, expected_brown_energy):
    task = Task(1, task_runtime, task_power)
    brown_energy = _calculate_brown_energy_of_task(task, task_start, current_green_events)
    assert brown_energy == expected_brown_energy


@pytest.mark.parametrize(
    'start, end, green_power_available, expected',
    [
        (5, 12, [(0, 100), (10, 200), (15, 0), (20, 130)], [(0, 100), (5, 200), (7, 200)]),
        (0, 20, [(0, 100), (10, 200), (15, 0), (20, 130)], [(0, 100), (10, 200), (15, 0), (20, 130)]),
        (0, 17, [(0, 100), (10, 200), (15, 0), (20, 130)], [(0, 100), (10, 200), (15, 0), (17, 0)]),
        (17, 20, [(0, 100), (10, 200), (15, 0), (20, 130)], [(0, 0), (3, 130)]),
    ]
)
def test_slice_green_power_available_list(start, end, green_power_available, expected):
    sliced = _slice_green_power_available_list(green_power_available, start, end)
    assert sliced == expected


@pytest.mark.parametrize(
    'expected_start_min, green_energy_available, task_runtime, task_power',
    [
        (10, [(0, 100), (10, 200), (15, 0), (20, 130)], 2, 200),
        (0, [(0, 100), (10, 200), (15, 0), (20, 130)], 10, 100),
        (0, [(0, 100), (10, 200), (15, 0), (20, 130)], 15, 100),
        (0, [(0, 100), (10, 200), (15, 0), (20, 130)], 20, 100),
        (8, [(0, 100), (10, 200), (15, 0), (20, 130)], 7, 200),
        (10, [(0, 100), (10, 200), (15, 0), (20, 130)], 5, 200),
        (0, [(0, 0), (10, 21)], 0, 0),
        (0, [(0, 20), (13, 40), (26, 30), (29, 40)], 15, 10),
    ]
)
def test_find_min_brown_energy_in_interval(expected_start_min, green_energy_available, task_runtime, task_power):
    task = Task(1, task_runtime, task_power)
    start_min = _find_min_brown_energy_in_interval(task, green_energy_available)
    assert start_min == expected_start_min
