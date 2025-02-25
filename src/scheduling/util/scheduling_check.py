def check(scheduling, graph):

    scheduling_violations = []

    for task in graph.list_of_tasks():

        start = scheduling[task.id][0]

        for pred in task.predecessors:
            pred_start = scheduling[pred.id][0]
            pred_finish_time = pred_start + pred.runtime

            if pred_finish_time > start:
                scheduling_violations.append(
                    (task.id, start, pred.id, pred_finish_time)
                )

    return scheduling_violations
