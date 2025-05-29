def sort_by_mean_runtime_unschedule(tasks, is_scheduled_task, right_mode=False):

    tasks_max_runtime = []

    for t in tasks:
        runtime_sum, count = calc_mean_runtime_child(t, is_scheduled_task, right_mode=right_mode)
        runtime_avg = runtime_sum / count
        tasks_max_runtime.append(
            (t, runtime_avg)
        )

    # Sort by degree and largest task first (LPT)
    tasks_max_runtime.sort(
        key=lambda d: (d[1], d[0].runtime),
        reverse=True
    )

    return [t for t, length in tasks_max_runtime]


def calc_mean_runtime_child(task, is_scheduled_task, right_mode=True):

    runtime_sum = 0

    task_to_check = task.successors if right_mode else task.predecessors
    count = 1 + len(task_to_check)

    for t in task_to_check:
        child_max_runtime, child_count = calc_mean_runtime_child(t, is_scheduled_task, right_mode=right_mode)
        runtime_sum += child_max_runtime
        count += child_count

    return runtime_sum + task.runtime, count


def sort_by_max_runtime_unschedule(tasks, is_scheduled_task, right_mode=False):

    tasks_max_runtime = []

    for t in tasks:
        max_runtime = calc_max_runtime_child(t, is_scheduled_task, right_mode=right_mode)
        tasks_max_runtime.append(
            (t, max_runtime)
        )

    # Sort by degree and largest task first (LPT)
    tasks_max_runtime.sort(
        key=lambda d: (d[1], d[0].runtime),
        reverse=True
    )

    return [t for t, length in tasks_max_runtime]


def calc_max_runtime_child(task, is_scheduled_task, right_mode=True):

    max_runtime = 0

    task_to_check = task.successors if right_mode else task.predecessors

    for t in task_to_check:
        child_max_runtime = calc_max_runtime_child(t, is_scheduled_task, right_mode=right_mode)
        if child_max_runtime > max_runtime:
            max_runtime = child_max_runtime

    if not is_scheduled_task(task.id) and task.runtime > max_runtime:
        max_runtime = task.runtime

    return max_runtime
