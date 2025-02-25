def calculate_left_boundary(task, scheduling):

    if len(task.predecessors) == 0:
        return 0, False

    is_limited_by_scheduled_predecessor = False
    max_earliest_predecessor_finish_time = -1
    for p in task.predecessors:

        p_earliest_finish_time = min_finish_time(p, scheduling)

        if p_earliest_finish_time > max_earliest_predecessor_finish_time:
            max_earliest_predecessor_finish_time = p_earliest_finish_time
            is_limited_by_scheduled_predecessor = (p.id in scheduling)

    return max_earliest_predecessor_finish_time, is_limited_by_scheduled_predecessor


def calculate_right_boundary(task, scheduling, deadline):
    if len(task.successors) == 0:
        return 0, False

    is_limited_by_scheduled_successor = False
    max_successor_start_time = -1
    for s in task.successors:

        s_max_start_time = max_start_time(s, scheduling, deadline)

        if s_max_start_time > max_successor_start_time:
            max_successor_start_time = s_max_start_time
            is_limited_by_scheduled_successor = (s.id in scheduling)

    return max_successor_start_time, is_limited_by_scheduled_successor


def min_finish_time(task, scheduling):
    if task.id in scheduling:
        start_time, machine = scheduling[task.id]
        return task.runtime + start_time

    if len(task.predecessors) == 0:
        return task.runtime

    max_predecessor_finish_time = -1
    for p in task.predecessors:
        p_earliest_finish_time = min_finish_time(p, scheduling)
        if p_earliest_finish_time > max_predecessor_finish_time:
            max_predecessor_finish_time = p_earliest_finish_time

    return task.runtime + max_predecessor_finish_time


def max_start_time(task, scheduling, deadline):
    if task.id in scheduling:
        start_time, machine = scheduling[task.id]
        return deadline - start_time

    if len(task.successors) == 0:
        return task.runtime

    max_successor_start_time = -1
    for s in task.successors:
        s_min_start_time = max_start_time(s, scheduling, deadline)
        if s_min_start_time > max_successor_start_time:
            max_successor_start_time = s_min_start_time

    return task.runtime + max_successor_start_time

