def calc_levels(graph):
    levels = {}

    visited_tasks = {}

    task = graph.get_task(graph.start_task_id)
    max_level = calc_levels_recursive(task, 0, levels, visited_tasks)

    return levels, max_level


def calc_levels_recursive(task, current_level, levels, visited_tasks):

    print(task.id)

    if task.id in visited_tasks:
        return visited_tasks[task.id]

    if not (task.id in levels) or levels[task.id] < current_level:
        levels[task.id] = current_level

    next_level = current_level + 1
    current_max_level = next_level

    for s in task.successors:
        max_level = calc_levels_recursive(s, next_level, levels, visited_tasks)
        if max_level > current_max_level:
            current_max_level = max_level

    visited_tasks[task.id] = current_max_level
    return current_max_level
