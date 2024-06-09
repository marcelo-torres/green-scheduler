import math

from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.algorithms.highest_power_first.drawer import Drawer
from src.scheduling.energy_usage_calculator import EnergyUsageCalculator
from src.task_graph.task_graph import TaskGraph


def create_graph():
    graph = TaskGraph()
    graph.set_start_task(0)
    graph.add_new_task(0, runtime=0, power=0) # Dummy task
    graph.add_new_task(1, runtime=10, power=14)
    graph.add_new_task(2, runtime=15, power=10)
    graph.add_new_task(3, runtime=20, power=12)
    graph.add_new_task(4, runtime=7, power=18)
    graph.add_new_task(5, runtime=14, power=14)
    graph.add_new_task(6, runtime=12, power=16)
    graph.add_new_task(7, runtime=8, power=4)

    graph.create_dependency(0, 1)
    graph.create_dependency(0, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(2, 3)
    graph.create_dependency(3, 6)
    graph.create_dependency(3, 4)
    graph.create_dependency(3, 5)
    graph.create_dependency(4, 6)
    graph.create_dependency(4, 7)
    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph


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
                if (init_o <= start_time <= end_o) or (init_o <= end_time <= end_o):
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


def find_min_brown_energy(task, lb, rb, scheduling, calculator):
    # TODO - to optimize this function

    start = lb

    start_min = start
    min_brown_energy_usage = float('inf')

    while start + task.runtime < rb:

        brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage(scheduling, task, start)

        if brown_energy_used < min_brown_energy_usage:
            min_brown_energy_usage = brown_energy_used
            start_min = start
        start += 1
    return start_min

def consolidate_tasks(scheduling):
    # TODO
    pass

def schedule_graph(graph, deadline, c=0.5):

    # green_energy = [2, 7, 10, 18, 23, 27, 30, 27, 24, 21, 18, 14]
    green_energy = [20, 40, 30, 20, 10, 5, 3, 2, 1, 4, 5, 6, 8]
    interval_size = 10

    scheduling = {}

    tasks = graph.list_of_tasks()

    # 1) Order all tasks by energy usage (power * runtime)
    tasks.sort(key=lambda t: t.power * t.runtime, reverse=True)

    boundary_calc = BoundaryCalculator(graph, deadline, c)
    energy_usage_calculator = EnergyUsageCalculator(graph, green_energy, interval_size)

    for task in tasks:

        # 2.1)  Calculate boundaries to avoid that a single task gets all slack time
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb + lvb
        rb = rcb + rvb

        # 2.2) Schedule each task when it uses less brown energy as early as possible
        scheduling[task.id] = find_min_brown_energy(task, lb, rb, scheduling, energy_usage_calculator)
        show_graph(lcb, lvb, rcb, rvb, deadline, green_energy, interval_size, scheduling, graph, task)
    consolidate_tasks(scheduling)

G = create_graph()
schedule_graph(G, 124)

