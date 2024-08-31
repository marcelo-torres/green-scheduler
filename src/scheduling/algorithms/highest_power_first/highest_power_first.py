from src.scheduling.algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling.drawer.drawer import Drawer
from src.scheduling.algorithms.highest_power_first.shift_left.shift_left import shift_left_tasks_to_save_energy, \
    shift_left_tasks_to_save_energy_greedy
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy_start_time import find_min_brown_energy, find_min_brown_energy_greedy

def draw_rectangles(drawer, events):
    max_power = -1
    active_tasks = []
    last_time = -1
    for task, time, event_type in events:
        if last_time >= 0 and time is not last_time:
            duration = time - last_time
            current_power = 0

            for active_task in active_tasks:
                drawer.add_rectangle(duration, active_task.power, last_time, current_power, description=active_task.id)
                current_power += active_task.power

            if current_power > max_power:
                max_power = current_power

        if event_type == 'start':
            active_tasks.append(task)
        else:
            active_tasks.remove(task)
        last_time = time

    return max_power

def draw_line(drawer, events):
    max_power = -1
    active_tasks = []
    time_list = []
    power_list = []
    last_time = -1
    for task, time, event_type in events:
        if last_time >= 0 and time is not last_time:
            duration = time - last_time
            current_power = 0
            for active_task in active_tasks:
                current_power += active_task.power
            time_list.append(last_time)
            time_list.append(last_time + duration)
            power_list.append(current_power)
            power_list.append(current_power)

            if current_power > max_power:
                max_power = current_power

        if event_type == 'start':
            active_tasks.append(task)
        else:
            active_tasks.remove(task)
        last_time = time

    # Add first point (to close the line)
    time_list.insert(0, time_list[0])  # duplicate the first time
    power_list.insert(0, 0)  # set power to 0 (y-axis)

    # Add last point (to close the line)
    time_list.append(time_list[-1])  # duplicate the last time (x-axis)
    power_list.append(0)  # set power to 0 (y-axis)

    drawer.add_line(time_list, power_list)
    return max_power

def create_graph(lcb, lvb, rcb, rvb, deadline, green_energy, interval_size, scheduling, graph, max_power=60):
    drawer = Drawer(max_power, deadline)
    drawer.add_constant_boundary(0, lcb)
    drawer.add_variable_boundary(lcb, lvb)
    drawer.add_variable_boundary(deadline - rvb - rcb, rvb)
    drawer.add_constant_boundary(deadline - rcb, rcb)

    interval_start = 0
    for power in green_energy:
        drawer.add_green_energy_availability(interval_start, interval_size, power)
        interval_start += interval_size

    events = []
    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)
        events.append(
            (task, start_time, 'start')
        )

        end_time = start_time + task.runtime
        events.append(
            (task, end_time, 'end')
        )

    # Sort by time and event type (start comes first)
    events.sort(key=lambda e: (e[1], 0 if e[2] == "start" else 1))

    is_a_lot_of_events = len(events) > 50
    if is_a_lot_of_events:
        max_power = draw_line(drawer, events)
    else:
        max_power = draw_rectangles(drawer, events)

    drawer.height = 1.5 * max_power
    return drawer


def get_task_ordering(key):
    task_ordering = {
        'energy': lambda t: t.power * t.runtime,
        'power': lambda t: t.power,
        'runtime': lambda t: t.runtime,
    }

    return task_ordering[key]


def schedule_graph(graph, deadline, green_power, interval_size, c=0.5, show=None, max_power=60, figure_file=None, task_ordering='energy'):

    scheduling = {}
    lcb = lvb = rcb = rvb = 0

    def show_graph_if(conditions): # TODO rename 'graph'
        if show in conditions:
            drawer = create_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power)
            drawer.show()

    def save_graph(file):
        drawer = create_graph(lcb, lvb, rcb, rvb, deadline, green_power, interval_size, scheduling, graph, max_power)
        drawer.save(file)

    tasks = graph.list_of_tasks()

    # 1) Order all tasks - default criteria: by energy usage (power * runtime)
    tasks.sort(key=get_task_ordering(task_ordering), reverse=True)

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
        #start_time = lb

        scheduling[task.id] = start_time
        energy_usage_calculator.add_scheduled_task(task, start_time)

        show_graph_if(['all'])

    show_graph_if(['all'])
    #shift_left_tasks_to_save_energy_greedy(graph, scheduling, boundary_calc, energy_usage_calculator)
    shift_left_tasks_to_save_energy(graph, scheduling, boundary_calc, deadline, energy_usage_calculator)

    show_graph_if(['last', 'all'])

    if figure_file:
        save_graph(figure_file)

    return scheduling

