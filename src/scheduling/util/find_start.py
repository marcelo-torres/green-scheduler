def find_min_start_machine(task, machines, max_predecessor_finish_time):

    min_start = float('inf')
    min_machine = None

    for machine in machines:
        start = max_predecessor_finish_time
        while not machine.can_schedule_task_in(task, start, start + task.runtime) and start < float('inf'): # Loop prevention
            start = machine.state.next_start(start) # TODO improve iteration strategy

        if start < min_start:
            min_start = start
            min_machine = machine

    return min_start, min_machine


def find_max_start_machine(task, machines, max_successor_start_time):

    max_end = float('-inf')
    max_machine = None

    for machine in machines:
        start = max_successor_start_time - task.runtime
        end = start + task.runtime
        while not machine.can_schedule_task_in(task, start, end) and start >= 0:
            end = machine.state.previous_start(end) # TODO improve iteration strategy
            start = end - task.runtime

        if end > max_end:
            max_end = end
            max_machine = machine

    max_stat = max_end - task.runtime

    return max_stat, max_machine
