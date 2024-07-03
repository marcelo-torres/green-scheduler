from src.scheduling.algorithms.highest_power_first.calc_levels import calc_levels


def calc_critical_path_length(graph):
    levels, max_level = calc_levels(graph)

    max_of_level = {}

    for task_id, level in levels.items():
        task = graph.get_task(task_id)

        if level not in max_of_level:
            max_of_level[level] = task.runtime
        else:
            current_max = max_of_level[level]
            if task.runtime > current_max:
                max_of_level[level] = task.runtime

    critical_path_length = 0
    for level, max_runtime in max_of_level.items():
        critical_path_length += max_runtime

    return critical_path_length