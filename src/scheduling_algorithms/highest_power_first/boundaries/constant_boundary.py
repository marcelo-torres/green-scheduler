def calculate_left_boundary(task, schedule):

    if len(task.predecessors) == 0:
        return 0

    max_predecessor_finish_time = -1
    for p in task.predecessors:

        if p.id in schedule:
            older_ft = schedule[p.id]
            # TODO return if a scheduled previous task is limiting
        else:
            older_ft = max_finish_time(p)

        if older_ft > max_predecessor_finish_time:
            max_predecessor_finish_time = older_ft

    return max_predecessor_finish_time


def calculate_right_boundary(task, schedule):
    # TODO take in account the scheduling
    if len(task.successors) == 0:
        return 0

    max_successor_start_time = -1
    for s in task.successors:
        if s.id in schedule:
            s_min_start_time = schedule[s.id]
        else:
            s_min_start_time = min_start_time(s)

        if s_min_start_time > max_successor_start_time:
            max_successor_start_time = s_min_start_time

    return max_successor_start_time


def max_finish_time(task):
    if len(task.predecessors) == 0:
        return task.runtime

    max_predecessor_finish_time = -1
    for p in task.predecessors:
        p_older_finish_time = max_finish_time(p)
        if p_older_finish_time > max_predecessor_finish_time:
            max_predecessor_finish_time = p_older_finish_time

    return task.runtime + max_predecessor_finish_time


def min_start_time(task):
    if len(task.successors) == 0:
        return task.runtime

    max_successor_start_time = -1
    for s in task.successors:
        s_min_start_time = min_start_time(s)
        if s_min_start_time > max_successor_start_time:
            max_successor_start_time = s_min_start_time

    return task.runtime + max_successor_start_time

