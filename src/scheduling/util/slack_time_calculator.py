from src.scheduling.util.topological_ordering import sort_topologically

def compute_min_start_time(graph):
    """
    Based on https://dl.acm.org/doi/pdf/10.1145/3491204.3527466
    :param graph:
    :return:
    """

    tasks = sort_topologically(graph, reverse=False)
    task_arrival_time = {
        tasks[0]: 0
    }

    for task_id in tasks:
        task = graph.get_task(task_id)

        arrival_time = task_arrival_time[task.id] if task.id in task_arrival_time else 0
        finish_time = arrival_time + task.runtime

        for child in task.successors:
            if child.id not in task_arrival_time or finish_time > task_arrival_time[child.id]:
                task_arrival_time[child.id] = finish_time

    return task_arrival_time


def calculate_slack_time(graph, min_start_time):
    """
    Based on https://dl.acm.org/doi/pdf/10.1145/3491204.3527466

    :param graph:
    :param min_start_time:
    :return:
    """

    slack = {}

    for task_id in sort_topologically(graph, reverse=True):
        task = graph.get_task(task_id)
        if len(task.successors) == 0:
            slack[task.id] = 0
        else:
            child_min = float('inf')
            for child in task.successors:
                child_slack = min_start_time[child.id]
                if child_slack < child_min:
                    child_min = child_slack

            # NOTE: The paper algorithm 2 hasa bug: its adding task.runtime instead of subtracting
            slack[task.id] = child_min - min_start_time[task.id] - task.runtime

    return slack