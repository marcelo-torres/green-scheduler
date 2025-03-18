from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.max_runtime_child import \
    sort_by_max_runtime_unschedule


def create_sort_by_max_predecessor_runtime(schedule, temp_schedule):
    is_scheduled_task = _create_is_scheduled_task(schedule, temp_schedule)
    return lambda task: sort_by_max_runtime_unschedule(task.predecessors, is_scheduled_task, right_mode=False)


def create_sort_by_max_successor_runtime(schedule, temp_schedule):
    is_scheduled_task = _create_is_scheduled_task(schedule, temp_schedule)
    return lambda task: sort_by_max_runtime_unschedule(task.successors, is_scheduled_task, right_mode=True)


def _create_is_scheduled_task(schedule, temp_schedule):
    return lambda task_id: task_id in schedule or task_id in temp_schedule


def unschedule(temp_schedule):
    for task_id, d in list(temp_schedule.items()):
        t, s, m = d
        m.unschedule_task(t, s)
