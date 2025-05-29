from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.lpt_boundary_estimator import \
    LptBoundaryEstimator
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_constant_left_boundary import \
    calculate_constant_left_boundary
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.makespan_calculator import calc_makespan

dummy_last_task_id = 'last_dummy_task'

OPTION_LPT = 'lpt'
OPTION_LCB = 'lcb'
OPTION_PATH_LPT = 'lcb-path-lpt'
OPTION_LCB_LPT = 'lcb-lpt'

def estimate_min_makespan_by_algorithm(graph, machines, algorithm):

    _add_dummy_task_to(graph)

    if algorithm == OPTION_LPT:
        makespan = _execute_lpt(graph, machines)

    elif algorithm == OPTION_LCB:
        makespan = _execute_left_boundary(graph, machines, False)

    elif algorithm == OPTION_PATH_LPT:
        makespan = _execute_left_boundary(graph, machines, True)

    elif algorithm == OPTION_LCB_LPT:
        makespan = _execute_left_boundary_lpt(graph, machines)


        makespan2 = _execute_lpt(graph, machines)
        makespan3 = _execute_right_boundary_lpt(graph, machines, makespan)

        print(makespan, makespan2, makespan3)
        assert makespan == makespan2
        assert makespan2 == makespan3

    else:
        raise Exception(f'Invalid algoritm {algorithm}')

    graph.remove_task(dummy_last_task_id)

    return makespan


def _get_tasks_without_children(graph):
    highest_rank_tasks = []

    for task in graph.list_of_tasks():
        if len(task.successors) == 0:
            highest_rank_tasks.append(task.id)

    return highest_rank_tasks


def _add_dummy_task_to(graph):

    tasks_without_children = _get_tasks_without_children(graph)

    graph.add_new_task(dummy_last_task_id, runtime=1, power=0)  # Dummy task

    for task_id in tasks_without_children:
        graph.create_dependency(task_id, dummy_last_task_id)


def _execute_lpt(graph, machines):
    power_series = PowerSeries('g1', [], 0)
    cluster = Cluster('c1', power_series, machines)
    schedule = lpt(graph, [cluster])

    return calc_makespan(schedule, graph) - graph.get_task(dummy_last_task_id).runtime

# def _execute_lpt2(graph, machines):
#     last_task = graph.get_task(dummy_last_task_id)
#     lpt_topological_sort = LtpTopologicalSort(graph)
#     estimator = LptBoundaryEstimator(machines, graph, lpt_topological_sort)
#     lcb, _ = estimator.calculate_constant_left_boundary(last_task, {})
#
#     return lcb


def _execute_left_boundary(graph, machines, use_path_lpt):
    last_task = graph.get_task(dummy_last_task_id)
    schedule_debug = {}
    lcb, _ = calculate_constant_left_boundary(last_task, {}, machines, use_lpt=use_path_lpt, schedule_debug=schedule_debug)
    # drawer = draw_scheduling(0, 0, 0, 0, lcb, [0] * 10, 100, schedule_debug, graph,
    #                          max_power=15)
    # drawer.show()
    return lcb


# def _execute_left_boundary_lpt(graph, machines):
#     lpt_topological_sort = LtpTopologicalSort(graph)
#     lpt_boundary_estimator = LptBoundaryEstimator(machines, graph, lpt_topological_sort)
#
#     last_task = graph.get_task(dummy_last_task_id)
#     lcb, _ = lpt_boundary_estimator.calculate_constant_left_boundary(last_task, {})
#
#     return lcb

def _execute_left_boundary_lpt(graph, machines):
    lpt_boundary_estimator = LptBoundaryEstimator(machines, graph)

    last_task = graph.get_task(dummy_last_task_id)
    lcb, _ = lpt_boundary_estimator.calculate_constant_left_boundary(last_task, {})

    return lcb

def _execute_right_boundary_lpt(graph, machines, deadline):
    lpt_boundary_estimator = LptBoundaryEstimator(machines, graph)

    first_task = graph.get_first_task()
    rcb, _ = lpt_boundary_estimator.calculate_constant_right_boundary(first_task, {}, deadline)

    return rcb