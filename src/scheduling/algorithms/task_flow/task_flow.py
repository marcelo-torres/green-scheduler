from src.scheduling.algorithms.highest_power_first.boundaries.single_machine.boundary import BoundaryCalculator
from src.scheduling.algorithms.highest_power_first.drawer.highest_power_first_drawer import draw_scheduling
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.energy.find_min_brown_energy import find_min_brown_energy
from src.scheduling.util.slack_time_calculator import compute_min_start_time, calculate_slack_time


def _split_tasks(slack):

    critical_tasks = []
    non_critical_tasks = []

    for task_id, slack_time in slack.items():
        if slack_time == 0:
            critical_tasks.append(task_id)
        else:
            non_critical_tasks.append(task_id)

    return critical_tasks, non_critical_tasks


def _sum_runtime(task_ids, graph):
    runtime = 0
    for task_id in task_ids:
        task = graph.get_task(task_id)
        runtime += task.runtime

    return runtime




def task_flow_schedule(graph, clusters, show='None', max_power=None, chart_x_end=None, graph_boundaries=True):

    # 1) Split tasks in critical and noncritical tasks
    min_start_time = compute_min_start_time(graph)
    slack = calculate_slack_time(graph, min_start_time)
    critical_tasks, non_critical_tasks = _split_tasks(slack)

    # The deadline is the critical path length
    deadline = _sum_runtime(critical_tasks, graph)

    scheduling = {}


    for cluster in clusters:
        lcb = lvb = rcb = rvb = 0

        green_power = clusters[0].power_series.green_power_list
        interval_size = clusters[0].power_series.interval_length

        def show_draw_if(conditions):
            if show in conditions:
                x_end = deadline if chart_x_end is None else chart_x_end
                drawer = draw_scheduling(lcb, lvb, rcb, rvb, x_end, green_power, interval_size, scheduling, graph,
                                         max_power=max_power, show_boundaries=graph_boundaries)
                drawer.show()

        boundary_calc = BoundaryCalculator(graph, deadline, 0)
        energy_usage_calculator = EnergyUsageCalculator(green_power, interval_size)

        # 2) Schedule critical path
        for task_id in critical_tasks:
            task = graph.get_task(task_id)
            start_time = min_start_time[task.id]

            scheduling[task.id] = start_time, None # TODO machine
            energy_usage_calculator.add_scheduled_task(task, start_time)

            show_draw_if(['all'])

        # 3) Schedule noncritical path tasks
        for task_id in non_critical_tasks:
            task = graph.get_task(task_id)
            # Calculate boundaries to avoid that a single task gets all slack time
            lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
            lb = lcb + lvb
            rb = rcb + rvb

            # Schedule each task when it uses less brown energy as early as possible
            start_time, brown_energy = find_min_brown_energy(task, lb, rb, deadline,
                                               energy_usage_calculator.get_green_power_available())

            scheduling[task.id] = start_time, None # TODO machine
            energy_usage_calculator.add_scheduled_task(task, start_time)

            show_draw_if(['all'])

        lcb = lvb = rcb = rvb = 0
        show_draw_if(['last', 'all'])

        return scheduling


# def task_flow_schedule_old(graph, green_power, interval_size, show='None', max_power=None, chart_x_end=None, graph_boundaries=True):
#
#     # 1) Split tasks in critical and noncritical tasks
#     min_start_time = compute_min_start_time(graph)
#     slack = calculate_slack_time(graph, min_start_time)
#     critical_tasks, non_critical_tasks = _split_tasks(slack)
#
#     # The deadline is the critical path length
#     deadline = _sum_runtime(critical_tasks, graph)
#
#     scheduling = {}
#     lcb = lvb = rcb = rvb = 0
#
#     def show_draw_if(conditions):
#         if show in conditions:
#             x_end = deadline if chart_x_end is None else chart_x_end
#             drawer = draw_scheduling(lcb, lvb, rcb, rvb, x_end, green_power, interval_size, scheduling, graph,
#                                      max_power=max_power, show_boundaries=graph_boundaries)
#             drawer.show()
#
#     boundary_calc = BoundaryCalculator(graph, deadline, 0)
#     energy_usage_calculator = EnergyUsageCalculator(green_power, interval_size)
#
#     # 2) Schedule critical path
#     for task_id in critical_tasks:
#         task = graph.get_task(task_id)
#         start_time = min_start_time[task.id]
#
#         scheduling[task.id] = start_time
#         energy_usage_calculator.add_scheduled_task(task, start_time)
#
#         show_draw_if(['all'])
#
#     # 3) Schedule noncritical path tasks
#     for task_id in non_critical_tasks:
#         task = graph.get_task(task_id)
#         # Calculate boundaries to avoid that a single task gets all slack time
#         lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, scheduling)
#         lb = lcb + lvb
#         rb = rcb + rvb
#
#         # Schedule each task when it uses less brown energy as early as possible
#         start_time = find_min_brown_energy(task, lb, rb, deadline, energy_usage_calculator.get_green_power_available())
#
#         scheduling[task.id] = start_time
#         energy_usage_calculator.add_scheduled_task(task, start_time)
#
#         show_draw_if(['all'])
#
#     lcb = lvb = rcb = rvb = 0
#     show_draw_if(['last', 'all'])
#
#     return scheduling
