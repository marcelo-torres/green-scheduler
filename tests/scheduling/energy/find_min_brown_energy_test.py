import pytest

from src.scheduling.energy.find_min_brown_energy import _slice_green_power_available_list, \
    _find_min_brown_energy_in_interval, _calculate_brown_energy_of_task
from src.scheduling.task_graph.task import Task

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
def test_find_min_brown_energy(expected_start_min, green_energy_available, task_runtime, task_power):
    task = Task(1, task_runtime, task_power)
    start_min = _find_min_brown_energy_in_interval(task, green_energy_available)
    assert start_min == expected_start_min


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
