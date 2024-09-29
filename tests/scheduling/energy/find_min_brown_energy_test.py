import pytest

from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy import _slice_green_power_available_list, \
    _find_min_brown_energy_in_interval, _calculate_brown_energy_of_task, IntervalException, \
    find_min_brown_energy
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
    'expected_start_min, lb, rb, deadline, green_power, green_interval_size, task, scheduled_tasks, description',
    [
        (0, 0, 0, 50, [0, 0, 0, 0, 0, 0], 10, Task(1, 17, 100), [], 'no green energy'),
        (13, 0, 0, 100, [1, 4, 5, 0, 10, 0], 5, Task(1, 12, 9), [], 'task spans over intervals'),
        (0, 0, 0, 100, [5, 2, 5, 2], 10, Task(1, 10, 5), [], 'task fits'),
        (0, 0, 0, 100, [2, 0, 1, 2, 0], 10, Task(1, 1, 3), [], 'min at start'),
        (30, 0, 0, 100, [2, 0, 4, 5], 10, Task(1, 1, 5), [], 'min at end'),
        (2, 0, 0, 100, [1, 2, 3, 2, 1, 2, 3, 2], 2, Task(1, 5, 5), [], 'never fits'),
        (7, 0, 0, 100, [0, 0, 0, 0, 1, 0, 0], 2, Task(1, 3, 100), [], 'only place'),
        (0, 0, 0, 100, [10, 15, 20, 30], 10, Task(1, 3, 31), [(0, Task(2, 40, 100))], 'other task takes all the green energy'),
        (5, 0, 0, 100, [5, 10, 5, 5], 10, Task(1, 15, 10), [], 'earliest min start'),
        (5, 0, 0, 100, [20, 10, 5, 5], 10, Task(1, 15, 10), [(0, Task(2, 10, 15))], 'earliest min start with scheduled task'),
        (10, 0, 0, 100, [20, 10, 5, 5], 10, Task(1, 15, 10), [(0, Task(2, 10, 16))], 'earliest min start with scheduled task with bigger task'),
        (27000, 0, 0, 192163,
         90*[0] + [420, 540, 630, 780, 960, 1050, 1020, 1050, 1110, 1020, 870, 840, 1260, 1260, 1680, 1530, 2520, 2430, 3570, 3720, 2220, 2010, 1800, 1530, 1380, 990, 900, 2640, 2310, 3330, 3840, 4590, 4560, 5760, 8040, 15750, 6210, 7140, 7590, 7020, 7710, 8850, 9150, 10170, 9750, 9660, 13800, 10020, 7710, 18330, 8310, 10500, 21210, 15210, 20040, 11850, 9210, 13170, 11550, 12150, 28470, 32580, 18630, 25740, 10200, 37890, 12690, 17910, 7980, 18030, 7260, 11730, 8190, 10320, 11700, 12270, 13350, 12960, 21360, 36810, 8700, 10800, 14070, 6510, 6240, 11820, 6480, 9900, 22440, 40110, 21510, 13170, 12750, 11190, 10890, 16380, 11220, 15780, 23490, 10470, 11640, 9960, 22020, 14400, 33570, 15030, 10560, 17250, 7860, 31770, 9900, 8550, 6690, 4710, 4020, 9660, 13200, 9120, 8820, 8580, 8160, 6660, 5760, 4440, 4200, 5370, 4500, 5460, 6570, 7950, 6810, 6390, 5160, 4050, 3150, 3900, 4680, 3510, 2610, 2100, 1680, 1650, 2340, 2400, 2040, 1440, 1620, 1950, 1710, 1470, 1290, 1080, 960, 870, 780, 690, 630, 570, 480, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 420, 480, 570, 600, 660, 720, 780, 840, 900, 1050, 1170, 1320, 1440, 2160, 2610, 3000, 3480, 4050, 4830, 5670, 6600, 7380, 8130, 8760, 9390, 10050, 10740, 11430, 12060, 12780, 13440, 13920, 14790, 15780, 15750, 17730, 19140, 13560, 3990, 17760, 20610, 4230, 23730, 23850, 24480],
         300, Task(1, 10571, 3.2138098727723934), [], 'float point problem - real bug')
    ]
)
def test_find_min_brown_energy_equal_to_greedy(expected_start_min, lb, rb, deadline, green_power, green_interval_size, task, scheduled_tasks, description):
    graph = TaskGraph()
    graph.add_task(task)

    energy_usage_calculator = EnergyUsageCalculator(green_power, green_interval_size)

    for start_time, scheduled_task in scheduled_tasks:
        graph.add_task(scheduled_task)
        energy_usage_calculator.add_scheduled_task(scheduled_task, start_time)

    start_min = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calculator.get_green_power_available())
    start_min_greedy = find_min_brown_energy_greedy(task, lb, rb, deadline, energy_usage_calculator)

    assert start_min == expected_start_min, f'{description} failed'
    assert start_min == start_min_greedy, f'{description} - equal to greedy failed'




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
