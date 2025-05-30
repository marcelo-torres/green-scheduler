from src.scheduling.algorithms.bounded_boundary_search.boundaries.multi_machine.multi_machine_boundary import \
    MultiMachineBoundaryCalculator
from src.scheduling.algorithms.bounded_boundary_search.boundaries.single_machine.boundary import BoundaryCalculator
from src.scheduling.algorithms.bounded_boundary_search.drawer.bounded_boundary_search_drawer import draw_scheduling
from src.scheduling.algorithms.bounded_boundary_search.shift.shift import shift_tasks_to_save_energy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.schedule_in_min_brown_energy import schedule_min_brown_energy_min_start

BOUNDARY_SINGLE = 'single'
BOUNDARY_DEFAULT = 'default'
BOUNDARY_LPT_PATH = 'lpt-path'
BOUNDARY_LPT = 'lpt'
BOUNDARY_LPT_FULL = 'lpt-full'

TASK_SORT_ENERGY = 'energy'
TASK_SORT_POWER = 'power'
TASK_SORT_RUNTIME = 'runtime'
TASK_SORT_RUNTIME_ASCENDING = 'runtime_ascending'

SHIFT_MODE_LEFT = 'left'
SHIFT_MODE_RIGHT_LEFT = 'right-left'
SHIFT_MODE_NONE = 'none'



def _get_task_ordering(key):
    task_ordering = {
        'energy': lambda t: t.power * t.runtime,
        'power': lambda t: t.power,
        'runtime': lambda t: t.runtime, #LPT
        'runtime_ascending': lambda t: -t.runtime, #SPT
    }

    return task_ordering[key]

def _validate_option(option, name, options):
    if option not in options:
        raise Exception(f"{name} '{option}' invalid")

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

# TODO - rename to bbs
def bbs(
        graph, deadline, c, clusters,
        task_sort=TASK_SORT_ENERGY,
        shift_mode=SHIFT_MODE_LEFT,
        boundary_strategy=BOUNDARY_DEFAULT,
        use_sort_scheduled=False,
        show=None,
        max_power=None,
        figure_file=None):

    _validate_option(shift_mode, 'shift mode', [SHIFT_MODE_LEFT, SHIFT_MODE_RIGHT_LEFT, SHIFT_MODE_NONE])
    _validate_option(boundary_strategy, 'boundary strategy', [BOUNDARY_SINGLE, BOUNDARY_DEFAULT, BOUNDARY_LPT_PATH, BOUNDARY_LPT, BOUNDARY_LPT_FULL])
    _validate_option(task_sort, 'task sort strategy', [TASK_SORT_ENERGY, TASK_SORT_POWER, TASK_SORT_RUNTIME, TASK_SORT_RUNTIME_ASCENDING])

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

    if boundary_strategy == BOUNDARY_SINGLE:
        boundary_calc = BoundaryCalculator(graph, deadline, c)
    else:
        boundary_calc = MultiMachineBoundaryCalculator(graph, deadline, c, machines, strategy=boundary_strategy)
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
