from src.scheduling.model.machine import CORES_PER_TASK


def calculate_constant_left_boundary(task, schedule, machine):

    temp_schedule = {}

    if len(task.predecessors) == 0:
        return 0, False

    is_limited_by_scheduled_predecessor = False
    max_earliest_predecessor_finish_time = -1
    for p in task.predecessors:

        p_earliest_finish_time = _min_finish_time(p, schedule, machine, temp_schedule)

        if p_earliest_finish_time > max_earliest_predecessor_finish_time:
            max_earliest_predecessor_finish_time = p_earliest_finish_time
            is_limited_by_scheduled_predecessor = (p.id in schedule)

    start = _find_min_task_start_with_enough_cores(task, machine, max_earliest_predecessor_finish_time)

    for task_id, d in list(temp_schedule.items()):
        t, s, m = d
        machine.unschedule_task(t, s)

    return start, is_limited_by_scheduled_predecessor


def _min_finish_time(task, schedule, machine, temp_schedule):
    if task.id in schedule:
        start_time = schedule[task.id]
        return task.runtime + start_time

    if task.id in temp_schedule:
        _, start_time, _ = temp_schedule[task.id]
        return task.runtime + start_time

    if len(task.predecessors) == 0:
        start = _find_and_schedule_min(task, machine, 0, temp_schedule)
        return start + task.runtime

    max_predecessor_finish_time = -1
    for p in task.predecessors:
        p_earliest_finish_time = _min_finish_time(p, schedule, machine, temp_schedule)
        if p_earliest_finish_time > max_predecessor_finish_time:
            max_predecessor_finish_time = p_earliest_finish_time

    start = _find_and_schedule_min(task, machine, max_predecessor_finish_time, temp_schedule)

    return start + task.runtime


def _find_and_schedule_min(task, machine, max_predecessor_finish_time, temp_schedule):
    start = _find_min_task_start_with_enough_cores(task, machine, max_predecessor_finish_time)
    machine.schedule_task(task, start)

    temp_schedule[task.id] = (task, start, machine.id)

    return start


def _find_min_task_start_with_enough_cores(task, machine, max_predecessor_finish_time):
    # TODO - improve search for time slot with available cores
    start = max_predecessor_finish_time
    while machine.state.min_free_cores_in(start, start + task.runtime) < CORES_PER_TASK:
        start = machine.state.next_start(start)

    return start
