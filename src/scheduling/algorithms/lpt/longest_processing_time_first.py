from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort


def lpt(graph, clusters):

    cluster = clusters[0] # TODO - implement multi-cluster
    machines = cluster.machines_list

    schedule = {}

    sorter = LtpTopologicalSort(graph)

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
