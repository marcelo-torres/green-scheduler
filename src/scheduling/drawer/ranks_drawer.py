import matplotlib
import matplotlib.pyplot as plt
from matplotlib import patches

from src.scheduling.util.topological_ordering import calculate_upward_rank

#matplotlib.use("qt5agg")  #https://stackoverflow.com/a/52221178


def _add_rectangle(ax, start, length, y, height, label, edgecolor = (0, 0, 1, 0.9), facecolor = (0, 0, 1, 0.5)):

    rect = patches.Rectangle((start, y), length, height, linewidth=1,
                             edgecolor=edgecolor, facecolor=facecolor)
    if label:
        rx, ry = rect.get_xy()
        cx = rx + rect.get_width() / 2.0
        cy = ry + rect.get_height() / 2.0
        ax.annotate(label, (cx, cy), color='black', weight='bold', fontsize=10, ha='center', va='center')

    ax.add_patch(rect)

def _get_rank_largest_task(graph, scheduling):
    rank_min_start_and_max_finish = {}

    ranks = calculate_upward_rank(graph)
    for task_id, rank in ranks.items():
        task = graph.get_task(task_id)

        task_start_time = scheduling[task_id]
        task_finish_time = task_start_time + task.runtime

        if rank not in rank_min_start_and_max_finish:
            rank_min_start_and_max_finish[rank] = (task_start_time, task_finish_time)
        else:
            rank_min_start, rank_max_finish = rank_min_start_and_max_finish[rank]
            if task_start_time < rank_min_start:
                rank_min_start = task_start_time
            if task_finish_time > rank_max_finish:
                rank_max_finish = task_finish_time
            rank_min_start_and_max_finish[rank] = (rank_min_start, rank_max_finish)

    return rank_min_start_and_max_finish, ranks
def draw(graph, scheduling):
    rank_min_start_and_max_finish, ranks = _get_rank_largest_task(graph, scheduling)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    task_height = 0.5
    height = 2

    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)
        rank = ranks[task_id]
        _add_rectangle(ax, start_time, task.runtime, rank * height, task_height, None, facecolor = (1, 0, 0, 0.5))

    workflow_finish_time = -1
    for rank, d in rank_min_start_and_max_finish.items():
        rank_min_start, rank_max_finish = d
        print(rank, rank_min_start, rank_max_finish)
        length = rank_max_finish - rank_min_start
        _add_rectangle(ax, rank_min_start, length, rank*height, height, rank)

        if rank_max_finish > workflow_finish_time:
            workflow_finish_time = rank_max_finish

    plt.xlim([0, workflow_finish_time * 1.5])
    plt.ylim([0, (height * len(rank_min_start_and_max_finish)) * 1.5])
    plt.show()
    matplotlib.pyplot.close()

