def _calculate_upward_rank_recursive(task, current_rank, ranks):

    if task.id not in ranks or ranks[task.id] < current_rank:
        ranks[task.id] = current_rank

    if len(task.successors) > 0:
        for successor in task.successors:
            if successor.id not in ranks or ranks[successor.id] < current_rank+1:
                _calculate_upward_rank_recursive(successor, current_rank + 1, ranks)


def calculate_upward_rank(graph):
    ranks = {}
    _calculate_upward_rank_recursive(graph.get_first_task(), 0, ranks)
    return ranks


def sort_topologically(graph, reverse=False):
    ranks = calculate_upward_rank(graph)

    task_rank_list = list(ranks.items())
    task_rank_list.sort(key=lambda d: d[1], reverse=reverse)

    tasks = [task_rank[0] for task_rank in task_rank_list]
    return tasks
