def calc_makespan(scheduling, graph):

    max_finish_time = -1

    for task_id, d in scheduling.items():
        start_time, machine = d
        task = graph.get_task(task_id)
        finish_time = start_time + task.runtime

        if finish_time > max_finish_time:
            max_finish_time = finish_time

    return max_finish_time