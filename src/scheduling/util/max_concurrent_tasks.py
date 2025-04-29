from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine


def count_max_parallel_tasks(graph):
    cluster = _create_cluster_for(graph)
    schedule = lpt(graph, [cluster])
    events = _to_event_list(schedule, graph)
    return _count_max_parallel_events(events)

def _create_cluster_for(graph):
    machines = [
        Machine('id1', cores=len(graph.list_of_tasks())),
    ]

    return Cluster('id', None, machines)

def _to_event_list(schedule, graph):
    events = []

    for task_id, data in schedule.items():
        start_time, _ = data

        task = graph.get_task(task_id)
        finish_time = start_time + task.runtime

        events.append(
            (start_time, 1)
        )
        events.append(
            (finish_time, -1)
        )

    events.sort(key=lambda e: e[0])

    return events

def _count_max_parallel_events(events):

    max_parallel_tasks = 0
    current_parallel_tasks = 0

    events += [(0, 0)]

    previous_time = -1
    for event_time, event_type in events:
        current_parallel_tasks += event_type

        if previous_time == event_time:
            continue # Consolidate all interval time before checking the max

        if current_parallel_tasks > max_parallel_tasks:
            max_parallel_tasks = current_parallel_tasks

        previous_time = event_time
    return max_parallel_tasks
