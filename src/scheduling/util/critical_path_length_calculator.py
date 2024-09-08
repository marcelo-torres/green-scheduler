from src.scheduling.util.calc_levels import calc_levels
from src.scheduling.task_graph.task_graph import TaskGraph
from src.scheduling.util.topological_ordering import sort_topologically

def calc_critical_path_length(graph):
    max_of_level = _get_max_of_level(graph)

    critical_path_length = 0
    for level, max_runtime in max_of_level.items():
        critical_path_length += max_runtime

    return critical_path_length


def _earliest_start_time(tasks, graph):
    start_times = {graph.get_first_task().id: 0}

    for task_id in tasks:
        task = graph.get_task(task_id)
        start_time = start_times[task.id] if task.id in start_times else 0
        finish_time = start_time + task.runtime
        for child in task.successors:
            if child.id not in start_times or finish_time > start_times[child.id]:
                start_times[child.id] = finish_time
    return start_times


def _slack_time(tasks, graph):

    start_times = _earliest_start_time(tasks, graph)

    slacks = {}
    # Calc slack time
    for task_id in tasks:
        task = graph.get_task(task_id)
        if len(task.successors) == 0:
            slacks[task.id] = 0

        else:
            min_children_start_time = float('inf')
            for child in task.successors:
                if start_times[child.id] < min_children_start_time:
                    min_children_start_time = start_times[child.id]

            slacks[task.id] = min_children_start_time - start_times[task.id] - task.runtime
    return slacks


def _get_max_of_level(graph):
    tasks = sort_topologically(graph)

    slacks = _slack_time(tasks, graph)

    # If there are two parallel paths withs tasks with the same runtime, then both paths can be critical.
    # Get only one task with slack time = 0 per critical path
    levels, max_level = calc_levels(graph)
    max_of_level = {}
    for task_id, level in levels.items():
        task = graph.get_task(task_id)
        if slacks[task_id] == 0:
            if level not in max_of_level:
                max_of_level[level] = task.runtime
            else:
                current_max = max_of_level[level]
                if task.runtime > current_max:
                    max_of_level[level] = task.runtime
    return max_of_level
