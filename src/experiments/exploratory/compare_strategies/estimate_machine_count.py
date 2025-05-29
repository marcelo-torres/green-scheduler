from math import ceil

from src.experiments.main.makespan_estimator_impl import OPTION_LCB, OPTION_PATH_LPT, OPTION_LCB_LPT, \
    estimate_min_makespan_by_algorithm, OPTION_LPT
from src.scheduling.algorithms.highest_power_first.highest_power_first import BOUNDARY_DEFAULT, BOUNDARY_LPT_PATH, \
    BOUNDARY_LPT
from src.scheduling.model.machine_factory import create_machines_with_target_resource, estimate_machine_count
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length


def _get_total_runtime(graph):
    total_runtime = 0
    for task in graph.list_of_tasks():
        total_runtime += task.runtime

    return total_runtime

def get_machines_count(graph, cores_per_machine, target_utilization, boundary_strategy):

    total_runtime = _get_total_runtime(graph)
    critical_path_length = calc_critical_path_length(graph)

    deadline_temp = critical_path_length + ceil((total_runtime - critical_path_length))

    machines = create_machines_with_target_resource(graph, total_runtime, [cores_per_machine],
                                                    target_utilization)
    # return len(machines)

    algorithm_map = {
        BOUNDARY_DEFAULT: OPTION_LCB,
        BOUNDARY_LPT_PATH: OPTION_PATH_LPT,
        BOUNDARY_LPT: OPTION_LCB_LPT,
    }

    algorithm = algorithm_map[boundary_strategy]

    min_makespan = estimate_min_makespan_by_algorithm(graph, machines, algorithm)

    return estimate_machine_count(graph, min_makespan, cores_per_machine, target_utilization), min_makespan
