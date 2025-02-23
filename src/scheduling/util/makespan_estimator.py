from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.topological_ordering import sort_topologically


# TODO testcif machine is full!!!
def estimate_min_makespan(graph, machines):
    temp_schedule = {}
    unschedule_list = []

    tasks = sort_topologically(graph)

    for task_id in tasks:
        task = graph.tasks[task_id]
        min_s = _min_start(task, temp_schedule, graph)
        start, machine = _find_min_start(task, min_s, machines)

        temp_schedule[task.id] = start
        machine.schedule_task(task, start)

        unschedule_list.append(
            (task, start, machine)
        )

    min_makespan = calc_makespan(temp_schedule, graph)

    _unschedule(unschedule_list)

    return min_makespan

def _min_start(task, temp_schedule, graph):
    max_pred_ft = 0
    for pred in task.predecessors:
        ft = temp_schedule[pred.id] + pred.runtime
        if ft > max_pred_ft:
            max_pred_ft = ft
    return max_pred_ft

def _find_min_start(task, min_start, machines):
    min_s = float('inf')
    min_machine = None

    for machine in machines:
        start = min_start
        while not machine.can_schedule_task_in(task, start, start + task.runtime) and start < float('inf'):
            start = machine.state.next_start(start)

        if start < min_s:
            min_s = start
            min_machine = machine

    return min_s, min_machine


def _unschedule(unschedule_list):
    for task, start, machine in unschedule_list:
        machine.unschedule_task(task, start)
