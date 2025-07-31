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


def min_finish_time(task, scheduling, memo=None):

    if memo is None:
        memo = {}

    if task.id in memo:
        return memo[task.id]

    if task.id in scheduling:
        start_time, machine = scheduling[task.id]
        finish_time = task.runtime + start_time
        return finish_time

    if len(task.predecessors) == 0:
        memo[task.id] = task.runtime
        return task.runtime

    max_predecessor_finish_time = -1
    for p in task.predecessors:
        p_earliest_finish_time = min_finish_time(p, scheduling, memo)
        if p_earliest_finish_time > max_predecessor_finish_time:
            max_predecessor_finish_time = p_earliest_finish_time

    finish_time = task.runtime + max_predecessor_finish_time
    memo[task.id] = finish_time
    return finish_time


def max_start_time(task, scheduling, deadline, memo=None):
    if memo is None:
        memo = {}

    if task.id in memo:
        return memo[task.id]

    if task.id in scheduling:
        start_time, _ = scheduling[task.id]
        value = deadline - start_time
        memo[task.id] = value
        return value

    if len(task.successors) == 0:
        memo[task.id] = task.runtime
        return task.runtime

    max_successor_start_time = -1
    for s in task.successors:
        s_min_start_time = max_start_time(s, scheduling, deadline, memo)
        if s_min_start_time > max_successor_start_time:
            max_successor_start_time = s_min_start_time

    value = task.runtime + max_successor_start_time
    memo[task.id] = value
    return value

