def find_min_start_machine(task, machines, max_predecessor_finish_time, end_limit=float('inf')):

    inf = float('inf')

    min_start = inf
    min_machine = None

    for machine in machines:
        interval_iterator = machine.search_intervals_to_schedule_task(task, max_predecessor_finish_time, end_limit)
        start, end = next(interval_iterator, (inf, inf))
        if start < min_start:
            min_start = start
            min_machine = machine

    return min_start, min_machine


def find_max_start_machine(task, machines, max_successor_start_time, start_limit=0):

    max_end = float('-inf')
    max_machine = None

    for machine in machines:
        start = max_successor_start_time - task.runtime
        end = start + task.runtime

        while start >= start_limit and not machine.can_schedule_task_in(task, start, end):
            end = machine.state.previous_start(end) # TODO improve iteration strategy to be like find_min_start_machine
            start = end - task.runtime

        if end > max_end and start >= start_limit:
            max_end = end
            max_machine = machine

    max_start = max_end - task.runtime

    return max_start, max_machine
