from src.scheduling.model.machine import CORES_PER_TASK


def calculate_constant_right_boundary(task, schedule, machines, deadline):

    if len(task.successors) == 0:
        return 0, False

    temp_schedule = {}

    min_successor_start_time = float('inf')
    min_successor = None
    for s in task.successors:
        s_max_start_time = max_start_time(s, schedule, machines, deadline, temp_schedule)

        if s_max_start_time < min_successor_start_time:
            min_successor_start_time = s_max_start_time
            min_successor = s

    is_limited_by_scheduled_successor = (min_successor.id in schedule)

    start, _ = _find_machine(task, machines, min_successor_start_time)
    _unschedule(temp_schedule)

    return deadline - (start + task.runtime), is_limited_by_scheduled_successor


def max_start_time(task, schedule, machine, deadline, temp_schedule):
    if task.id in schedule:
        start_time = schedule[task.id]
        #return deadline - start_time
        return start_time

    if task.id in temp_schedule:
        _, start_time, _ = temp_schedule[task.id]
        #return deadline - start_time
        return start_time

    if len(task.successors) == 0:
        start = _temp_schedule_task(task, machine, deadline, temp_schedule)
        #return start + task.runtime
        return start

    max_successor_start_time = -1
    for s in task.successors:
        s_max_start_time = max_start_time(s, schedule, machine, deadline, temp_schedule)
        if s_max_start_time > max_successor_start_time:
            max_successor_start_time = s_max_start_time

    start = _temp_schedule_task(task, machine, max_successor_start_time, temp_schedule)

    #return start + task.runtime
    return start


def _temp_schedule_task(task, machines, max_successor_start_time, temp_schedule):
    start, machine = _find_machine(task, machines, max_successor_start_time)

    machine.schedule_task(task, start)
    temp_schedule[task.id] = (task, start, machine)

    return start


def _find_machine(task, machines, max_successor_start_time):

    max_end = float('-inf')
    max_machine = None

    for machine in machines:
        start = max_successor_start_time - task.runtime
        end = start + task.runtime
        while not machine.can_schedule_task_in(task, start, end):
            end = machine.state.previous_start(end) # TODO improve iteration strategy
            start = end - task.runtime

        if end > max_end:
            max_end = end
            max_machine = machine

    return max_end - task.runtime, max_machine


def _unschedule(temp_schedule):
    for task_id, d in list(temp_schedule.items()):
        t, s, m = d
        print(task_id, t, s, m)
        m.unschedule_task(t, s)
