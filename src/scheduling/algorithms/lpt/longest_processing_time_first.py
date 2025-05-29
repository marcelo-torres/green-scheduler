from src.scheduling.algorithms.bounded_boundary_search.drawer.bounded_boundary_search_drawer import draw_scheduling
from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort


def lpt(graph, clusters, show='None', max_power=None, chart_x_end=None):

    cluster = clusters[0] # TODO - implement multi-cluster
    machines = cluster.machines_list

    schedule = {}
    sorter = LtpTopologicalSort(graph)

    def show_draw_if(conditions):
        if show in conditions:
            green_power = clusters[0].power_series.green_power_list
            interval_size = clusters[0].power_series.interval_length

            x_end = chart_x_end
            drawer = draw_scheduling(0, 0, 0, 0, x_end, green_power, interval_size, schedule, graph,
                                     max_power=max_power, show_boundaries=False)
            drawer.show()



    for task in sorter.get_lpt_topological_list():

        interval_start = _max_pred_finish_time(task, schedule)

        min_start = float('inf')
        min_machine = None

        for machine in machines:
            start, end = _get_first_interval(machine, task, interval_start)
            if start < min_start:
                min_start = start
                min_machine = machine

        min_machine.schedule_task(task, min_start)
        schedule[task.id] = min_start, min_machine.id
        show_draw_if(['all'])

    show_draw_if(['last', 'all'])

    return schedule

def _get_first_interval(machine, task, interval_start):
    # TODO implement method to search for first interval
    interval_iterator = machine.search_intervals_to_schedule_task(task, interval_start, float('inf'))
    start, end = next(interval_iterator)

    return start, end

def _max_pred_finish_time(task, schedule):

    max_finish_time = 0

    for pred in task.predecessors:
        if pred.id in schedule:
            start_time, _ = schedule[pred.id]
            finish_time = start_time + pred.runtime

            if finish_time > max_finish_time:
                max_finish_time = finish_time

    return max_finish_time
