from src.scheduling.model.machine import CORES_PER_TASK
from src.scheduling.util.find_start import find_max_start_machine


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

    start, _ = find_max_start_machine(task, machines, min_successor_start_time)
    _unschedule(temp_schedule)

    return deadline - (start + task.runtime), is_limited_by_scheduled_successor


def max_start_time(task, schedule, machine, deadline, temp_schedule):
    if task.id in schedule:
        start_time = schedule[task.id][0]
        return start_time

    if task.id in temp_schedule:
        _, start_time, _ = temp_schedule[task.id]
        return start_time

    if len(task.successors) == 0:
        start = _temp_schedule_task(task, machine, deadline, temp_schedule)
        return start

    min_successor_earliest_st = float('inf')
    for s in task.successors:
        s_max_start_time = max_start_time(s, schedule, machine, deadline, temp_schedule)
        if s_max_start_time < min_successor_earliest_st:
            min_successor_earliest_st = s_max_start_time

    start = _temp_schedule_task(task, machine, min_successor_earliest_st, temp_schedule)

    return start


def _temp_schedule_task(task, machines, max_successor_start_time, temp_schedule):
    start, machine = find_max_start_machine(task, machines, max_successor_start_time)

    machine.schedule_task(task, start)
    temp_schedule[task.id] = (task, start, machine)

    return start





def _unschedule(temp_schedule):
    for task_id, d in list(temp_schedule.items()):
        t, s, m = d
        m.unschedule_task(t, s)
