import math

from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.algorithms.highest_power_first.drawer import Drawer
from src.scheduling.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.topological_ordering import calculate_upward_rank, sort_topologically
from src.task_graph.task_graph import TaskGraph


def show_graph(lcb, lvb, rcb, rvb, deadline, green_energy, interval_size, scheduling, graph, task):
    drawer = Drawer(60, deadline)
    drawer.add_constant_boundary(0, lcb)
    drawer.add_variable_boundary(lcb, lvb)
    drawer.add_variable_boundary(deadline - rvb - rcb, rvb)
    drawer.add_constant_boundary(deadline - rcb, rcb)

    interval_start = 0
    for power in green_energy:
        drawer.add_green_energy_availability(interval_start, interval_size, power)
        interval_start += interval_size

    d = []
    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)
        end_time = start_time + task.runtime
        d.append(
            (task, start_time, end_time)
        )

    # Calculate task overlap to show tasks above each other
    for i in reversed(range(len(d))):
        task, start_time, end_time = d[i]

        overlap_locations = []

        # Find task overlaps for current task
        for j in reversed(range(i)):
            task_j, start_time_j, end_time_j = d[j]
            is_overlap = (start_time_j <= start_time <= end_time_j) or (start_time_j <= end_time <= end_time_j)
            if is_overlap:
                overlap_locations.append(
                    (start_time_j, task_j, 'start')
                )
                overlap_locations.append(
                    (end_time_j, task_j, 'end')
                )

        # Sort locations to represent a timeline with several intervals
        overlap_locations.sort(key=lambda e: e[0])

        max_power_overlapped_by_task = 0

        # Find the max sum of power of tasks that overlap with each other and with the current task
        if len(overlap_locations) > 0:
            start_time_o, task_o, type = overlap_locations.pop(0)
            init_o = start_time_o
            interval_power = task_o.power

            for time_o, task_o, type in overlap_locations:
                end_o = time_o

                # Check if the current interval overlaps with the current task
                if (init_o < start_time < end_o) or (init_o < end_time < end_o):
                    if interval_power > max_power_overlapped_by_task:
                        max_power_overlapped_by_task = interval_power

                if type == 'start':
                    # If a new task is starting, then more power is needed
                    interval_power += task_o.power
                else:
                    # If a task is turning off, then less power is needed
                    interval_power -= task_o.power

                init_o = end_o

        drawer.add_scheduled_task(start_time, task, max_power_overlapped_by_task)

    #drawer.add_task(schedule[task.id], task)
    drawer.show()


def find_min_brown_energy(task, lb, rb, scheduling, deadline, calculator):
    # TODO - to optimize this function

    start = lb

    start_min = start
    min_brown_energy_usage = float('inf')

    while start + task.runtime <= deadline - rb:

        brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage(scheduling, task, start)

        if brown_energy_used < min_brown_energy_usage:
            min_brown_energy_usage = brown_energy_used
            start_min = start
        start += 1
    return start_min


def consolidate_tasks(graph, scheduling, boundary_calc, energy_usage_calc):
    # TODO

    # Order tasks by topological order
    tasks = sort_topologically(graph)
    for i in range(len(tasks)):
        task = graph.get_task(tasks[i])

        scheduling_temp = scheduling.copy()
        scheduling_temp.pop(task.id)

        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling_temp)

        start_time = scheduling[task.id]
        min_brown_energy_used = energy_usage_calc.calculate_energy_usage(scheduling_temp, task, start_time)[0]

        for j in range(start_time-lcb):

            new_start_time = lcb + j
            brown_energy_used = energy_usage_calc.calculate_energy_usage(scheduling_temp, task, new_start_time)[0]
            if brown_energy_used <= min_brown_energy_used and new_start_time < scheduling[task.id]:
                min_brown_energy_used = brown_energy_used
                scheduling[task.id] = new_start_time

    # Iterate each task using Integer index
    #   Calculate boundary
    #   Calculate energy usage
    #   Try to shift left

    pass


def schedule_graph(graph, deadline, green_power, interval_size, c=0.5, show=None):

    scheduling = {}

    tasks = graph.list_of_tasks()

    # 1) Order all tasks by energy usage (power * runtime)
    tasks.sort(key=lambda t: t.power * t.runtime, reverse=True)

    boundary_calc = BoundaryCalculator(graph, deadline, c)
    energy_usage_calculator = EnergyUsageCalculator(graph, green_power, interval_size)

    for task in tasks:

        # 2.1)  Calculate boundaries to avoid that a single task gets all slack time
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb + lvb
        rb = rcb + rvb

        # 2.2) Schedule each task when it uses less brown energy as early as possible
        scheduling[task.id] = find_min_brown_energy(task, lb, rb, scheduling, deadline, energy_usage_calculator)

        if show == 'all':
            show_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, task)

    if show == 'last' or show == 'all':
        show_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, task)

    # brown_energy_used = energy_usage_calculator.calculate_energy_usage(scheduling)[0]
    # last_task = graph.get_task(7)
    # makespan = scheduling[last_task.id] + last_task.runtime
    # print(f'brown_energy_used before consolidation: {brown_energy_used}J | makespan: {makespan}s')

    consolidate_tasks(graph, scheduling, boundary_calc, energy_usage_calculator)

    if show == 'last' or show == 'all':
        show_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, task)

    return scheduling

