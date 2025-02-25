from src.scheduling.util.makespan_calculator import calc_makespan


def calc_stretch(graph, scheduling, makespan=None, ignore_fake_task=True):

    if makespan is None:
        makespan = calc_makespan(scheduling, graph)

    if ignore_fake_task:
        scheduling = scheduling.copy()
        fake_task = graph.get_first_task()
        del scheduling[fake_task.id]

    scheduling_items = list(scheduling.items())
    scheduling_items.sort(key=lambda schedule: schedule[1])  # Sort by start time

    workflow_start_time = scheduling_items[0][1][0] if len(scheduling_items) > 0 else 0

    return makespan - workflow_start_time
