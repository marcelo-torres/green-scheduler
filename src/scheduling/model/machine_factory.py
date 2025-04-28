import math

from src.scheduling.model.machine import CORES_PER_TASK, Machine


def create_machines_with_target(graph, deadline, cores_per_machine, target_utilization):
    return create_machines_with_target_resource(graph, deadline, cores_per_machine, target_utilization)


def create_machines_with_target_resource(graph, duration, cores_per_machine, target_utilization):
    """
    2022 | TaskFlow: An Energy- and Makespan-Aware Task Placement Policy for Workflow Scheduling through Delay Management
    https://doi.org/10.1145/3491204.3527466

    Original implementation available on https://github.com/atlarge-research/wta-sim/blob/tasks-across-machines/src/main/kotlin/science/atlarge/wta/simulator/WTASim.kt

    :param graph
    :param cores_per_machine:
    :param target_utilization:
    :param duration:
    :return:
    """

    machines = []

    for cores in cores_per_machine:
        machines_count = estimate_machine_count(graph, duration, cores, target_utilization)

        for i in range(machines_count):
            machine_id = f'c{cores}_m{i}'
            machines.append(
                Machine(machine_id, cores=cores)
            )

    return machines


def estimate_machine_count(graph, duration, cores, target_utilization):
    total_resource_usage = get_total_resource_required(graph)
    return math.ceil(total_resource_usage / (duration * cores * target_utilization))


def get_total_resource_required(graph):
    total_resource_usage = 0
    for task in graph.list_of_tasks():
        total_resource_usage += task.runtime * CORES_PER_TASK

    return total_resource_usage