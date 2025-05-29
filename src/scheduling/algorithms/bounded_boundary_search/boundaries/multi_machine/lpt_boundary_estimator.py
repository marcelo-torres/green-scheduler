from itertools import chain

from src.scheduling.util.find_start import find_min_start_machine, find_max_start_machine
from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort


class LptBoundaryEstimator:

    def __init__(self, machines, graph):
        self.machines = machines
        self.graph = graph

        self.sorter = LtpTopologicalSort(self.graph)

        self.independent_tasks = _map_independent_tasks(self.graph)

    def calculate_constant_left_boundary(self, task, schedule):
        tasks_to_remove = []
        shifted_schedule = {}

        for t in self.sorter.lpt_topological_list_until(task):
            if t.id in schedule:
                shifted_schedule[t.id] = schedule[t.id]
                continue

            max_pred_finish_time, _ = _max_pred_finish_time(t, shifted_schedule)
            min_start, min_machine = find_min_start_machine(t, self.machines, max_pred_finish_time)

            if min_machine is None:
                raise Exception(f'No machine found to schedule task {t}')

            _temp_schedule(t, min_start, min_machine, shifted_schedule, tasks_to_remove)

        max_pred_finish_time, _ = _max_pred_finish_time(task, shifted_schedule)
        min_start, _ = find_min_start_machine(task, self.machines, max_pred_finish_time)

        lcb, is_max_predecessor_scheduled = min_start, False  # TODO - temp

        _unschedule(tasks_to_remove)

        return lcb, is_max_predecessor_scheduled


    def calculate_constant_right_boundary(self, task, schedule, deadline):
        tasks_to_remove = []
        shifted_schedule = {}

        independent_tasks = self.independent_tasks[task.id]
        topo_list = []
        for _, t in self.sorter.lpt_topological_list:
            if t in independent_tasks:
                topo_list.append(t)

        for t in chain(self.sorter.lpt_topological_inverse_list_until(task), topo_list):
            if t.id in schedule:
                shifted_schedule[t.id] = schedule[t.id]
                continue

            min_succ_start_time, _ = _min_successor_start_time(t, schedule, deadline)
            max_start, max_machine = find_max_start_machine(t, self.machines, min_succ_start_time)

            if max_machine is None:
                raise Exception(f'No machine found to schedule task {t}')

            _temp_schedule(t, max_start, max_machine, shifted_schedule, tasks_to_remove)

        min_succ_start_time, _ = _min_successor_start_time(task, schedule, deadline)
        max_start, _ = find_max_start_machine(task, self.machines, min_succ_start_time)

        rcb, is_min_successor_scheduled = deadline - (max_start+task.runtime), True  # TODO - temp

        _unschedule(tasks_to_remove)

        return rcb, is_min_successor_scheduled


def _temp_schedule(task, min_start, machine, temp_schedule, tasks_to_remove):
    temp_schedule[task.id] = min_start, machine.id
    machine.schedule_task(task, min_start)
    tasks_to_remove.append(
        (machine, task, min_start)
    )

def _unschedule(tasks_to_remove):
    for machine, task, start in tasks_to_remove:
        machine.unschedule_task(task, start)

def _max_pred_finish_time(task, schedule):

    max_finish_time = 0
    max_pred = None

    for pred in task.predecessors:
        if pred.id in schedule:
            start_time, _ = schedule[pred.id]
            finish_time = start_time + pred.runtime

            if finish_time > max_finish_time:
                max_finish_time = finish_time
                max_pred = pred

    return max_finish_time, max_pred

def _min_successor_start_time(task, schedule, deadline):
    min_start_time = deadline
    min_successor = None

    for succ in task.successors:
        if succ.id in schedule:
            start_time, _ = schedule[succ.id]
            if start_time + succ.runtime < deadline:
                if start_time < min_start_time:
                    min_start_time = start_time
                    min_successor = succ

    return min_start_time, min_successor

def _map_independent_tasks(graph):

    independent_tasks = {}

    for task in graph.list_of_tasks():
        independent_tasks[task.id] = []

        successors = _get_all_successors(task)
        predecessors = _get_all_predecessors(task)

        for t in graph.list_of_tasks():
            if t not in successors and t not in predecessors:
                independent_tasks[task.id].append(t)

    return independent_tasks

def _get_all_successors(task):
    successors = []
    for succ in task.successors:
        successors.append(succ)
        successors.extend(
            _get_all_successors(succ)
        )
    return successors

def _get_all_predecessors(task):
    predecessors = []
    for pred in task.predecessors:
        predecessors.append(pred)
        predecessors.extend(
            _get_all_predecessors(pred)
        )
    return predecessors