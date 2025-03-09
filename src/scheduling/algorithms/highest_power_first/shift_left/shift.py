from src.scheduling.util.schedule_in_min_brown_energy import schedule_min_brown_energy_min_start, schedule_min_brown_energy_max_start
from src.scheduling.util.topological_ordering import sort_topologically, sort_topologically_scheduled_tasks


def shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calc, mode='left'):
    _validate_mode(mode)

    machines_map = _to_machine_map(machines)

    # Order tasks by topological order
    tasks = sort_topologically_scheduled_tasks(graph, scheduling, reverse=(mode == 'right'))


    for i in range(len(tasks)):

        task = graph.get_task(tasks[i])

        _remove_task(task, scheduling, machines_map, energy_usage_calc)

        if mode == 'right':
            schedule_min_brown_energy_max_start(task, machines, scheduling, deadline, boundary_calc, energy_usage_calc, ignore_v_boundary=True)
        else:
            schedule_min_brown_energy_min_start(task, machines, scheduling, deadline, boundary_calc, energy_usage_calc, ignore_v_boundary=True)

def _remove_task(task, scheduling, machines_map, energy_usage_calc):
    energy_usage_calc.remove_scheduled_task(task)
    start_time, machine_id = scheduling[task.id]
    machine = machines_map[machine_id]
    machine.unschedule_task(task, start_time)
    del scheduling[task.id]

def _to_machine_map(machines):
    machines_map = {}

    for machine in machines:
        machines_map[machine.id] = machine

    return machines_map


def _validate_mode(mode):

    available_modes = ['left', 'right']

    if mode not in available_modes:
        raise Exception(f'Invalid mode {mode}')
