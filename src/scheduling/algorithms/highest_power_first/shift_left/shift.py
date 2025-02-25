from src.scheduling.energy.find_min_brown_energy import find_min_brown_energy
from src.scheduling.util.topological_ordering import sort_topologically


def shift_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calc, mode='left'):
    _validate_mode(mode)

    # Order tasks by topological order
    tasks = sort_topologically(graph, reverse=(mode == 'right'))

    max_start_mode = (mode == 'right')

    for i in range(len(tasks)):
        task = graph.get_task(tasks[i])

        # Remove task from temp scheduling and from energy calculator
        energy_usage_calc.remove_scheduled_task(task)
        del scheduling[task.id]

        # Compute boundaries (do not consider variable boundaries)
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb
        rb = rcb

        # Find new start
        g_power = energy_usage_calc.get_green_power_available()
        new_start_time = find_min_brown_energy(task, lb, rb, deadline, g_power, max_start_mode=max_start_mode)
        scheduling[task.id] = new_start_time, None # TODO machine
        energy_usage_calc.add_scheduled_task(task, new_start_time)


def _validate_mode(mode):

    available_modes = ['left', 'right']

    if mode not in available_modes:
        raise Exception(f'Invalid mode {mode}')
