import statistics


def _convert_scheduling_to_events(scheduling, graph):
    events = []
    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)

        events.append(
            (start_time, 'start')
        )

        end_time = start_time + task.runtime
        events.append(
            (end_time, 'end')
        )
    events.sort(key=lambda e: e[0])  # Order by start time
    return events


def _active_tasks_by_time(events):
    active_tasks_count = 0

    previous = (0, 0)
    active_tasks_by_time = []
    active_tasks = []

    for event_time, event_type in events:
        if event_type == 'start':
            active_tasks_count += 1
        else:
            active_tasks_count -= 1

        previous_time = previous[0]

        if previous_time != event_time:
            active_tasks_by_time.append(previous)
            active_tasks.append(previous[1])
        previous = (event_time, active_tasks_count)

    active_tasks_by_time.append(previous)
    active_tasks.append(previous[1])

    return active_tasks_by_time, active_tasks


def count_active_tasks(scheduling, graph):
    events = _convert_scheduling_to_events(scheduling, graph)
    active_tasks_by_time, active_tasks = _active_tasks_by_time(events)

    max_active_tasks = 0
    sum_of_all = 0

    for count in active_tasks:
        if count > max_active_tasks:
            max_active_tasks = count
        sum_of_all += count

    mean = sum_of_all / len(active_tasks_by_time)

    std = statistics.stdev(active_tasks)

    return max_active_tasks, mean, std, active_tasks_by_time


