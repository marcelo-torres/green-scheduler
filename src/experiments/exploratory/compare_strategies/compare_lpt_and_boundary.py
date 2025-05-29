import random

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.lpt_boundary_estimator import \
    LptBoundaryEstimator
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_constant_left_boundary import \
    calculate_constant_left_boundary
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check
from src.util.stopwatch import Stopwatch

resources_path = '../../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

MIN_TASK_POWER_DEFAULT = 1
MAX_TASK_POWER_DEFAULT = 5

dummy_last_task_id = 'last_dummy_task'

random.seed(15735667867885)


def random_uniform():
    return random.uniform(MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)


def get_last_level_tasks(graph):
    highest_rank_tasks = []

    for task in graph.list_of_tasks():
        if len(task.successors) == 0:
            highest_rank_tasks.append(task.id)

    return highest_rank_tasks


def add_dummy_task_to_last_level(graph):

    highest_rank_tasks = []

    for task in graph.list_of_tasks():
        if len(task.successors) == 0:
            highest_rank_tasks.append(task.id)

    graph.add_new_task(dummy_last_task_id, runtime=1, power=0)  # Dummy task

    for task_id in highest_rank_tasks:
        graph.create_dependency(task_id, dummy_last_task_id)


def load_graph(workflow_provider):
    graph = workflow_provider(random_uniform)

    add_dummy_task_to_last_level(graph)

    return graph


def execute_lpt(graph, cluster):
    schedule = lpt(graph, [cluster])

    violations = check(schedule, graph)
    if len(violations) > 0:
        raise Exception('Violations found in LPT: ', violations)

    return calc_makespan(schedule, graph) - graph.get_task(dummy_last_task_id).runtime


def execute_left_boundary(graph, machines, use_path_lpt):
    last_task = graph.get_task(dummy_last_task_id)
    lcb, _ = calculate_constant_left_boundary(last_task, {}, machines, use_lpt=use_path_lpt)
    return lcb


def execute_left_boundary_with_lpt(graph, machines):
    lpt_topological_sort = LtpTopologicalSort(graph)
    lpt_boundary_estimator = LptBoundaryEstimator(machines, graph, lpt_topological_sort)

    last_task = graph.get_task(dummy_last_task_id)
    lcb, _ = lpt_boundary_estimator.calculate_constant_left_boundary(last_task, {})

    return lcb


def execute_right_boundary_with_lpt(graph, machines, deadline):
    lpt_topological_sort = LtpTopologicalSort(graph)
    lpt_boundary_estimator = LptBoundaryEstimator(machines, graph, lpt_topological_sort)

    first_task = graph.get_first_task()
    rcb, _ = lpt_boundary_estimator.calculate_constant_right_boundary(first_task, {}, deadline)

    return rcb


def create_cluster(interval_size, green_power_provider, cores_per_machine, machines_count):
    green_power = green_power_provider[1]()
    power_series = PowerSeries('g1', green_power, interval_size)

    machines = []
    for i in range(machines_count):
        machine_id = f'c{cores_per_machine}_m{i}'
        machines.append(
            Machine(machine_id, cores=cores_per_machine)
        )

    return Cluster('c1', power_series, machines)


def compare_approaches(graph, cluster_provider):
    stop_watch = Stopwatch()

    stop_watch.start()
    cluster = cluster_provider()
    lpt_makespan = execute_lpt(graph, cluster)
    lpt_duration = stop_watch.get_elapsed_time()

    stop_watch.start()
    cluster = cluster_provider()
    lcb_makespan = execute_left_boundary(graph, cluster.machines_list, False)
    lcb_duration = stop_watch.get_elapsed_time()

    stop_watch.start()
    cluster = cluster_provider()
    lcb_lpt_makespan = execute_left_boundary(graph, cluster.machines_list, True)
    lcb_lpt_duration = stop_watch.get_elapsed_time()

    stop_watch.start()
    cluster = cluster_provider()
    lcb_new_lpt_makespan = execute_left_boundary_with_lpt(graph, cluster.machines_list)
    lcb_new_lpt_duration = stop_watch.get_elapsed_time()

    stop_watch.start()
    cluster = cluster_provider()
    deadline = lcb_new_lpt_makespan + graph.get_task(dummy_last_task_id).runtime
    rcb_new_lpt_makespan = deadline - execute_right_boundary_with_lpt(graph, cluster.machines_list, deadline)
    rcb_new_lpt_duration = stop_watch.get_elapsed_time()

    return {
        'lpt_makespan': lpt_makespan,
        'lpt_duration': lpt_duration,
        'lcb_makespan': lcb_makespan,
        'lcb_duration': lcb_duration,
        'lcb_lpt_makespan': lcb_lpt_makespan,
        'lcb_lpt_duration': lcb_lpt_duration,
        'lcb_new_lpt_makespan': lcb_new_lpt_makespan,
        'lcb_new_lpt_duration': lcb_new_lpt_duration,
        'rcb_new_lpt_makespan': rcb_new_lpt_makespan,
        'rcb_new_lpt_duration': rcb_new_lpt_duration,
    }


def report(headers, data=None):

    separator = '\t'
    sorted_data = []

    if data is None:
        print(separator.join(headers))
        return

    for header in headers:
        if header in data:
            sorted_data.append(
                str(data[header])
            )

    print(separator.join(sorted_data))


def evaluate_approaches(workflow_providers, cluster_provider):

    headers = ['workflow', 'lpt_makespan', 'lcb_makespan', 'lcb_lpt_makespan', 'lcb_new_lpt_makespan', 'rcb_new_lpt_makespan',
               'lpt_duration', 'lcb_duration', 'lcb_lpt_duration', 'lcb_new_lpt_duration', 'rcb_new_lpt_duration']
    report(headers)

    for workflow_name, workflow_provider in workflow_providers:
        graph = load_graph(workflow_provider)
        results = compare_approaches(graph, cluster_provider)

        results['workflow'] = workflow_name

        report(headers, results)


def execute():
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovolta_reader = PhotovoltaReader(resources_path)

    interval_size = 300  # 300 seconds = 5 minutes
    green_power_provider = ('trace-1', lambda: photovolta_reader.get_trace_1(size=30))

    cores_per_machine = 1
    machines_count = 2

    cluster_provider = lambda: create_cluster(interval_size, green_power_provider, cores_per_machine, machines_count)

    workflow_providers = [
        ('blast', lambda random_power: wfcommons_reader.read_blast_workflow(1000, 11.4492900609, random_power)),
        ('bwa', lambda random_power: wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, random_power)),
        ('cycles', lambda random_power: wfcommons_reader.read_cycles_workflow(1000, 31.0991735531, random_power)),
        ('genome', lambda random_power: wfcommons_reader.read_genome_workflow(1000, 35.7812995246, random_power)),
        ('soykb', lambda random_power: wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, random_power)),
        ('srasearch', lambda random_power: wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583, random_power)),
        ('montage', lambda random_power: wfcommons_reader.read_montage_workflow(1000, 11.17646556189, random_power)),
        ('seismology', lambda random_power: wfcommons_reader.read_seismology_workflow(1000, 4000, random_power)),
    ]

    evaluate_approaches(workflow_providers, cluster_provider)


if __name__ == '__main__':
    execute()
