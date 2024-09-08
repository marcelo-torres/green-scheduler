from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.algorithms.highest_power_first.drawer.highest_power_first_drawer import draw_scheduling
from src.scheduling.algorithms.highest_power_first.shift_left.shift_left import shift_left_tasks_to_save_energy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy import find_min_brown_energy


def get_task_ordering(key):
    task_ordering = {
        'energy': lambda t: t.power * t.runtime,
        'power': lambda t: t.power,
        'runtime': lambda t: t.runtime,
    }

    return task_ordering[key]


def schedule_graph(graph, deadline, green_power, interval_size, c=0.5, show=None, max_power=60, figure_file=None,
                   task_ordering='energy'):
    scheduling = {}
    lcb = lvb = rcb = rvb = 0

    def show_draw_if(conditions):
        if show in conditions:
            drawer = draw_scheduling(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph,
                                  max_power)
            drawer.show()

    def save_draw(file):
        drawer = draw_scheduling(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power)
        drawer.save(file)

    tasks = graph.list_of_tasks()

    # 1) Order all tasks - default criteria: by energy usage (power * runtime)
    tasks.sort(key=get_task_ordering(task_ordering), reverse=True)

    boundary_calc = BoundaryCalculator(graph, deadline, c)
    energy_usage_calculator = EnergyUsageCalculator(green_power, interval_size)

    for task in tasks:
        # 2.1)  Calculate boundaries to avoid that a single task gets all slack time
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
        lb = lcb + lvb
        rb = rcb + rvb

        # 2.2) Schedule each task when it uses less brown energy as early as possible
        start_time = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calculator.get_green_power_available())

        scheduling[task.id] = start_time
        energy_usage_calculator.add_scheduled_task(task, start_time)

        show_draw_if(['all'])

    show_draw_if(['all'])
    shift_left_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calculator)

    show_draw_if(['last', 'all'])

    if figure_file:
        save_draw(figure_file)

    return scheduling
