from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_shared import \
    create_sort_by_max_predecessor_runtime, unschedule, copy_from_temp_schedule
from src.scheduling.util.find_start import find_min_start_machine


def calculate_constant_left_boundary(task, schedule, machines, use_lpt=False, same_level_tasks=None, schedule_debug=None):

    if len(task.predecessors) == 0:
        return 0, False

    temp_schedule = {}

    max_earliest_predecessor_finish_time = -1
    max_predecessor = None

    if use_lpt:
        sort_predecessors = create_sort_by_max_predecessor_runtime(schedule, temp_schedule)
    else:
        sort_predecessors = lambda t: t.predecessors

    for p in sort_predecessors(task):
        p_earliest_finish_time = _min_finish_time(p, schedule, machines, temp_schedule, sort_predecessors)

        if p_earliest_finish_time > max_earliest_predecessor_finish_time:
            max_earliest_predecessor_finish_time = p_earliest_finish_time
            max_predecessor = p

    if same_level_tasks is not None:
        for t in same_level_tasks:
            _min_finish_time(t, schedule, machines, temp_schedule, sort_predecessors)

    is_limited_by_scheduled_predecessor = (max_predecessor.id in schedule)

    start, _ = find_min_start_machine(task, machines, max_earliest_predecessor_finish_time)

    if schedule_debug is not None:
        schedule_debug.update(
            copy_from_temp_schedule(temp_schedule)
        )

    unschedule(temp_schedule)

    return start, is_limited_by_scheduled_predecessor


def _min_finish_time(task, schedule, machines, temp_schedule, sort_predecessors):
    if task.id in schedule:
        start_time = schedule[task.id][0]
        return task.runtime + start_time

    if task.id in temp_schedule:
        _, start_time, _ = temp_schedule[task.id]
        return task.runtime + start_time

    if len(task.predecessors) == 0:
        start = _temp_schedule_task(task, machines, 0, temp_schedule)
        return start + task.runtime

    max_predecessor_finish_time = -1
    for p in sort_predecessors(task):
        p_earliest_finish_time = _min_finish_time(p, schedule, machines, temp_schedule, sort_predecessors)
        if p_earliest_finish_time > max_predecessor_finish_time:
            max_predecessor_finish_time = p_earliest_finish_time

    start = _temp_schedule_task(task, machines, max_predecessor_finish_time, temp_schedule)

    return start + task.runtime


def _temp_schedule_task(task, machines, max_predecessor_finish_time, temp_schedule):
    start, machine = find_min_start_machine(task, machines, max_predecessor_finish_time)

    machine.schedule_task(task, start)
    temp_schedule[task.id] = (task, start, machine)

    return start


