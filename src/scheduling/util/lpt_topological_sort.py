from src.scheduling.util.topological_ordering import calculate_upward_rank


class LtpTopologicalSort:

    def __init__(self, graph):
        task_ranks = calculate_upward_rank(graph)
        self.task_ranks_map = task_ranks

        lpt_topological_list, lpt_topological_inverse_list = _calc_lists(graph, task_ranks)

        self.lpt_topological_list = lpt_topological_list
        self.lpt_topological_inverse_list = lpt_topological_inverse_list

    def get_lpt_topological_list(self):
        for t_rank, t in self.lpt_topological_list:
            yield t

    def lpt_topological_list_until(self, task):
        rank = self.get_task_rank(task.id)
        task_iter = iter(self.lpt_topological_list)

        t_rank, t = _next(task_iter)
        while t_rank is not None and t_rank < rank:
            yield t
            t_rank, t = _next(task_iter)

    def lpt_topological_inverse_list_until(self, task):
        rank = self.get_task_rank(task.id)
        task_iter = iter(self.lpt_topological_inverse_list)

        t_rank, t = _next(task_iter)
        while t_rank is not None and t_rank > rank:
            yield t
            t_rank, t = _next(task_iter)

    def get_task_rank(self, task_id):
        return self.task_ranks_map[task_id]


def _calc_lists(graph, task_ranks):
    lpt_topological_list = []
    lpt_topological_inverse_list = []

    for task_id, rank in task_ranks.items():
        task = graph.get_task(task_id)
        lpt_topological_list.append(
            (rank, task)
        )

        lpt_topological_inverse_list.append(
            (rank, task)
        )

    lpt_topological_list.sort(key=lambda d: (d[0], -d[1].runtime), reverse=False)
    lpt_topological_inverse_list.sort(key=lambda d: (d[0], d[1].runtime), reverse=True)

    return lpt_topological_list, lpt_topological_inverse_list


def _next(task_iterator):
    return next(task_iterator, (None, None))
