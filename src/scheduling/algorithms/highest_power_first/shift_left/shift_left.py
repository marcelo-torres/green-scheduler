from src.scheduling.energy.find_min_brown_energy_start_time import find_min_brown_energy
from src.scheduling.util.topological_ordering import sort_topologically


def shift_left_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calc):
    # Order tasks by topological order
    tasks = sort_topologically(graph)

    for i in range(len(tasks)):
        task = graph.get_task(tasks[i])
        start_time = scheduling[task.id]

        # Remove task from temp scheduling and from energy calculator
        energy_usage_calc.remove_scheduled_task(task, start_time)

        # Compute boundaries
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb + lvb
        rb = rcb + rvb

        # Find new start
        new_start_time = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calc.get_green_power_available())
        scheduling[task.id] = new_start_time


def shift_left_tasks_to_save_energy_greedy(graph, scheduling, boundary_calc, energy_usage_calc):
    # Order tasks by topological order
    tasks = sort_topologically(graph)

    for i in range(len(tasks)):
        task = graph.get_task(tasks[i])

        scheduling_temp = scheduling.copy()
        scheduling_temp.pop(task.id)

        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling_temp)

        start_time = scheduling[task.id]

        min_brown_energy_used = energy_usage_calc.calculate_energy_usage()[0]
        energy_usage_calc.remove_scheduled_task(task, start_time)

        for j in range(start_time-lcb):

            new_start_time = lcb + j

            energy_usage_calc.remove_scheduled_task(task, new_start_time)
            brown_energy_used = energy_usage_calc.calculate_energy_usage()[0]

            if brown_energy_used <= min_brown_energy_used and new_start_time < scheduling[task.id]:
                min_brown_energy_used = brown_energy_used
                scheduling[task.id] = new_start_time
            else:
                energy_usage_calc.remove_scheduled_task(task, new_start_time)
