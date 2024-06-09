def calc_levels(graph):
    levels = {}

    task = graph.get_task(graph.start_task_id)
    max_level = calc_levels_recursive(task, 0, levels)

    return levels, max_level


def calc_levels_recursive(task, current_level, levels):
    if not (task.id in levels) or levels[task.id] < current_level:
        levels[task.id] = current_level

    next_level = current_level + 1
    current_max_level = next_level

    for s in task.successors:
        max_level = calc_levels_recursive(s, next_level, levels)
        if max_level > current_max_level:
            current_max_level = max_level

    return current_max_level
