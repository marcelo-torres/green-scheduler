from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.drawer.drawer import Drawer
from src.scheduling.algorithms.highest_power_first.shift_left.shift_left import shift_left_tasks_to_save_energy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy_start_time import find_min_brown_energy


def show_graph(lcb, lvb, rcb, rvb, deadline, green_energy, interval_size, scheduling, graph, max_power=60):
    drawer = Drawer(max_power, deadline)
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
            is_overlap = (start_time_j < start_time < end_time_j) or (start_time_j < end_time < end_time_j)
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


def schedule_graph(graph, deadline, green_power, interval_size, c=0.5, show=None, max_power=60):

    scheduling = {}
    lcb = lvb = rcb = rvb = 0

    def show_graph_if(conditions):
        if show in conditions:
            show_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power)

    tasks = graph.list_of_tasks()

    # 1) Order all tasks by energy usage (power * runtime)
    tasks.sort(key=lambda t: t.power * t.runtime, reverse=True)

    boundary_calc = BoundaryCalculator(graph, deadline, c)
    energy_usage_calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    energy_usage_calculator.init()

    for task in tasks:

        # 2.1)  Calculate boundaries to avoid that a single task gets all slack time
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb + lvb
        rb = rcb + rvb

        # 2.2) Schedule each task when it uses less brown energy as early as possible
        #start_time = find_min_brown_energy_greedy(task, lb, rb, deadline, energy_usage_calculator)
        start_time = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calculator.get_green_power_available())

        scheduling[task.id] = start_time
        energy_usage_calculator.add_scheduled_task(task, start_time)

        show_graph_if(['all'])

    show_graph_if(['last', 'all'])

    # TODO - review consolidation bug (makespan lesser than minimum)
    #shift_left_tasks_to_save_energy_greedy(graph, scheduling, boundary_calc, energy_usage_calculator)
    shift_left_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calculator)

    show_graph_if(['last', 'all'])

    return scheduling

