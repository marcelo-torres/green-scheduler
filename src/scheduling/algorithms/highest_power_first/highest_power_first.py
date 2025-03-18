from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_boundary import \
    MultiMachineBoundaryCalculator
from src.scheduling.algorithms.highest_power_first.drawer.highest_power_first_drawer import draw_scheduling
from src.scheduling.algorithms.highest_power_first.shift_left.shift import shift_tasks_to_save_energy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.schedule_in_min_brown_energy import schedule_min_brown_energy_min_start


def _get_task_ordering(key):
    task_ordering = {
        'energy': lambda t: t.power * t.runtime,
        'power': lambda t: t.power,
        'runtime': lambda t: t.runtime,
        'runtime_ascending': lambda t: -t.runtime,
    }

    return task_ordering[key]

def _validate_shift_mode(mode):
    valid_modes = ['left', 'right-left', 'none']

    if mode not in valid_modes:
        raise Exception(f"shift mode '{mode}' invalid")


def _apply_shift(shift_mode, graph, scheduling, machines, boundary_calc, deadline, energy_usage_calculator, use_sort_scheduled):

    if shift_mode == 'none':
        return scheduling

    brown_energy_used, _, _ = energy_usage_calculator.calculate_energy_usage()
    previous_scheduling = scheduling.copy()

    if shift_mode == 'left':
        shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calculator, use_sort_scheduled=use_sort_scheduled)

    elif shift_mode == 'right-left':
        shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calculator, mode='right', use_sort_scheduled=use_sort_scheduled)
        shift_tasks_to_save_energy(graph, scheduling, machines, boundary_calc, deadline, energy_usage_calculator, mode='left', use_sort_scheduled=use_sort_scheduled)

    new_brown_energy_used, _, _ = energy_usage_calculator.calculate_energy_usage()
    if new_brown_energy_used <= brown_energy_used:
        return scheduling

    return previous_scheduling


def highest_power_first(graph, deadline, c, clusters, task_sort='energy', shift_mode='left', use_lpt_boundary=False, use_sort_scheduled=False, show=None, max_power=None, figure_file=None):
    _validate_shift_mode(shift_mode)

    scheduling = {}
    lcb = lvb = rcb = rvb = 0

    green_power = clusters[0].power_series.green_power_list
    interval_size = clusters[0].power_series.interval_length

    def show_draw_if(conditions):
        if show in conditions:
            drawer = draw_scheduling(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power=max_power)
            drawer.show()

    def save_draw(file):
        drawer = draw_scheduling(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power=max_power)
        drawer.save(file)

    tasks = graph.list_of_tasks()
    cluster = clusters[0] # TODO - implement multi-cluster
    machines = cluster.machines_list

    # Order all tasks - default criteria: by energy usage (power * runtime)
    tasks.sort(key=_get_task_ordering(task_sort), reverse=True)

    #boundary_calc = BoundaryCalculator(graph, deadline, c)
    boundary_calc = MultiMachineBoundaryCalculator(graph, deadline, c, machines, use_lpt=use_lpt_boundary)
    energy_usage_calculator = EnergyUsageCalculator(green_power, interval_size)

    for task in tasks:
        lcb, lvb, rcb, rvb = schedule_min_brown_energy_min_start(task, machines, scheduling, deadline, boundary_calc, energy_usage_calculator)
        show_draw_if(['all'])

    lcb = lvb = rcb = rvb = 0  # Reset boundaries to show final chart without boundaries
    show_draw_if(['all'])

    scheduling = _apply_shift(shift_mode, graph, scheduling, machines, boundary_calc, deadline, energy_usage_calculator, use_sort_scheduled)

    show_draw_if(['last', 'all'])

    if figure_file:
        save_draw(figure_file)

    return scheduling
