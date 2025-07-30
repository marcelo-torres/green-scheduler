import itertools
from datetime import datetime

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.experiments.shared.ParallelExperimentExecutor import ParallelExperimentExecutor
from src.experiments.shared.experiment_file_helper import create_csv_file, write_reports_to_csv
from src.experiments.main.generate_workflows_for_experiments import num_of_tasks_smaller, runtime_factor_map_smaller, \
    num_of_tasks_bigger, runtime_factor_map_bigger
from src.experiments.shared.random_utils import RandomProvider
from src.scheduling.algorithms.bounded_boundary_search.bounded_boundary_search import bbs, BOUNDARY_SINGLE
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check
from src.scheduling.util.stretch_calculator import calc_stretch
from src.util.stopwatch import Stopwatch
from src.util.time_utils import seconds_to_hours




def report_scheduling(scheduling, graph, energy_calculator, print_resport=False):
    makespan = calc_makespan(scheduling, graph)

    workflow_stretch = calc_stretch(graph, scheduling, makespan=makespan)
    scheduling_hash = hash(frozenset(scheduling.items()))

    brown_energy_used, green_energy_not_used, total_energy = energy_calculator.calculate_energy_usage_for_scheduling(
        scheduling, graph)
    green_energy_used = total_energy - brown_energy_used

    max_active_tasks, mean, std, active_tasks_by_time = count_active_tasks(scheduling, graph)

    if print_resport:
        print(f'\tMakespan: {makespan:,}s ({seconds_to_hours(makespan)})')
        print(f'\tWorkflow Stretch: {workflow_stretch:,}s ({seconds_to_hours(workflow_stretch)})')
        print()

        print(f'\tscheduling_hash: {scheduling_hash:}')
        print()

        print(f'\tBrown energy used: {brown_energy_used:,.{2}f}J')
        print(f'\t...Green energy used: {green_energy_used:,.{2}f}J')
        print(f'\t...Total energy used: {total_energy:,.{2}f}J')
        print(f'\tGreen energy not used: {green_energy_not_used:,.{2}f}J')
        print()


        print(f'\tmax_active_tasks: {max_active_tasks:,.{0}f}')
        print(f'\tactive_tasks_mean: {mean:,.{2}f}')
        print(f'\tactive_tasks_std: {std:,.{2}f}')
        print()

    return {
        'makespan': makespan,
        'workflow_stretch': workflow_stretch,
        'brown_energy_used': brown_energy_used,
        'green_energy_used': green_energy_used,
        'total_energy': total_energy,
        'green_energy_not_used': green_energy_not_used,
        'max_active_tasks': max_active_tasks,
        'active_tasks_mean': mean,
        'active_tasks_std': std,
        'scheduling_hash': scheduling_hash
    }


def schedule_and_report(graph, green_power, interval_size, min_makespan, deadline_factor, task_ordering, boundary_strategy,
                        cluster_factory, shift_mode='right-left',
                        c=0.0, show='off', print_resport=False):
    deadline = min_makespan * deadline_factor
    number_of_tasks = len(graph.tasks)

    if print_resport:
        print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')
        print(f'\tDeadline: {deadline:,}s ({seconds_to_hours(deadline)})')
        print(f'\tNumber of tasks: {number_of_tasks:,.{2}f}')

    cluster = cluster_factory()

    scheduling = bbs(graph, deadline, c, [cluster], task_sort=task_ordering, shift_mode=shift_mode, boundary_strategy=boundary_strategy, show=show)

    energy_calculator = EnergyUsageCalculator(green_power, interval_size)
    scheduling_report = report_scheduling(scheduling, graph, energy_calculator, print_resport=print_resport)

    pre_scheduling_report = {
        'min_makespan': min_makespan,
        'deadline': deadline,
        'number_of_tasks': number_of_tasks,
    }

    violations = check(scheduling, graph)

    post_scheduling_report = {
        'scheduling_violations': len(violations)
    }

    full_report = pre_scheduling_report | scheduling_report | post_scheduling_report

    if print_resport:
        print('\tViolations: ', check(scheduling, graph))
        print()

    return full_report


def experiments_per_workflow(experiment_parameters, metadata, parameters_report):

    graph = experiment_parameters['graph']
    green_power = experiment_parameters['green_power']
    interval_size = experiment_parameters['interval_size']
    power_distribution = experiment_parameters['power_distribution']
    iteration = experiment_parameters['iteration']
    cluster_factory = experiment_parameters['cluster_factory']
    min_makespan = experiment_parameters['min_makespan']
    boundary_strategy = experiment_parameters['boundary_strategy']

    shift_modes = experiment_parameters['shift_modes']
    deadline_factors = experiment_parameters['deadline_factors']
    c_values = experiment_parameters['c_values']
    task_sort_criterias = experiment_parameters['task_ordering_criterias']

    prefix = metadata['prefix']
    job_number = metadata['job_number']
    job_count = metadata['job_count']
    experiment_count = metadata['experiment_count']

    print(f'JOB {job_number} of {job_count} starting | Iteration {iteration} | {prefix}')

    reports = []
    i = 1

    for shift, d, c_value, sort in itertools.product(shift_modes, deadline_factors, c_values, task_sort_criterias):
        parameters_report_temp = parameters_report | {
            'experiment': f'J{job_number}_E{i}',
            'experiment_type': f'J{prefix}',
            'iteration': f'{iteration}',
            'shift_mode': shift,
            'deadline_factor': d,
            'c_value': c_value,
            'task_ordering': sort,
            'power_distribution': power_distribution,
            'job_number': job_number,
            'boundary_strategy': boundary_strategy,
        }

        report = schedule_and_report(graph, green_power, interval_size, min_makespan, d, sort, boundary_strategy,
                                     cluster_factory, c=c_value, shift_mode=shift)

        full_report = parameters_report_temp | report
        reports.append(full_report)

        i += 1

    print(f'JOB {job_number} finished')
    return reports

def len_product(*args):

    product = 1

    for list_to_multiply in args:
        product *= len(list_to_multiply)

    return product


def execute_experiments(resources_path, synthetic_path, random_provider):

    headers = [
        'job_number', 'experiment', 'experiment_type', 'iteration', 'workflow', 'energy_trace',
        'shift_mode', 'c_value', 'deadline_factor', 'deadline', 'boundary_strategy',
        'task_ordering', 'scheduling_hash', 'power_distribution',
        'min_makespan', 'makespan', 'workflow_stretch',
        'brown_energy_used', 'green_energy_used', 'total_energy', 'green_energy_not_used',
        'max_active_tasks', 'active_tasks_mean', 'active_tasks_std', 'number_of_tasks',
        'scheduling_violations',
    ]

    start_time = datetime.now()
    file_full_path = create_csv_file(resources_path, start_time, headers)

    def save_report(report):
        return write_reports_to_csv(report, headers, file_full_path)
    executor = ParallelExperimentExecutor(save_report)


    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovolta_reader = PhotovoltaReader(resources_path)
    interval_size = 300 # 300 seconds = 5 minutes

    #target_utilization = 0.4
    cores_per_machine = 1000

    random_functions = [
        ('uniform', random_provider.random_uniform),
        # ('gaussian', random_provider.random_gauss),
        # ('inverted_exponential', random_provider.random_expovariate_inverse),
    ]

    num_of_tasks = num_of_tasks_bigger
    runtime_factor_map = runtime_factor_map_bigger

    # num_of_tasks = num_of_tasks_smaller
    # runtime_factor_map = runtime_factor_map_smaller

    workflow_providers = [
        ('blast', lambda random_power, index: wfcommons_reader.read_blast_workflow(num_of_tasks, runtime_factor_map['blast'], random_power, index)),
        ('bwa', lambda random_power, index: wfcommons_reader.read_bwa_workflow(num_of_tasks, runtime_factor_map['bwa'], random_power, index)),
        ('cycles', lambda random_power, index: wfcommons_reader.read_cycles_workflow(num_of_tasks, runtime_factor_map['cycles'], random_power, index)),
        ('genome', lambda random_power, index: wfcommons_reader.read_genome_workflow(num_of_tasks, runtime_factor_map['genome'], random_power, index)),
        ('soykb', lambda random_power, index: wfcommons_reader.read_soykb_workflow(num_of_tasks, runtime_factor_map['soykb'], random_power, index)),
        ('srasearch', lambda random_power, index: wfcommons_reader.read_srasearch_workflow(num_of_tasks, runtime_factor_map['srasearch'], random_power, index)),
        ('montage', lambda random_power, index: wfcommons_reader.read_montage_workflow(num_of_tasks, runtime_factor_map['montage'], random_power, index)),
        ('seismology', lambda random_power, index: wfcommons_reader.read_seismology_workflow(num_of_tasks, runtime_factor_map['seismology'], random_power, index)),
    ]

    green_power_providers = [
        ('trace-1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace-2', lambda: photovolta_reader.get_trace_2(size=30)),
        ('trace-3', lambda: photovolta_reader.get_trace_3(size=30)),
        ('trace-4', lambda: photovolta_reader.get_trace_4(size=30)),
    ]

    # Process parameters
    experiment_repetitions = 10
    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [2, 4, 8]
    c_values = [0, 0.5, 0.8]
    task_ordering_criterias = ['energy', 'power', 'runtime', 'runtime_ascending']

    # experiment_repetitions = 2
    # shift_modes = ['none']
    # deadline_factors = [2]
    # c_values = [0]
    # task_ordering_criterias = ['runtime']

    boundary_strategies = [
        BOUNDARY_SINGLE,
        #BOUNDARY_DEFAULT,
        #BOUNDARY_LPT_PATH,
        #BOUNDARY_LPT,
    ]

    job_count = len_product(random_functions, workflow_providers, green_power_providers, boundary_strategies) * experiment_repetitions
    experiment_count = len_product(shift_modes, deadline_factors, c_values, task_ordering_criterias)

    print(f'{experiment_count} experiments will be executed in {job_count} jobs. Total: {job_count * experiment_count}\n')

    executor.start()

    stopwatch = Stopwatch()
    stopwatch.start()

    j = 1
    for distribution_name, random_function in random_functions:
        for workflow_name, workflow_provider in workflow_providers:
            for boundary_strategy in boundary_strategies:
                for i in range(experiment_repetitions):
                    graph = workflow_provider(random_function, i)

                    # machines_count, min_makespan = get_machines_count(graph, cores_per_machine, target_utilization, boundary_strategy)
                    machines_count = 1

                    # Estimate the min makespan
                    temp_cluster = create_cluster(machines_count, cores_per_machine, [], 0)
                    lpt_schedule = lpt(graph, [temp_cluster])
                    min_makespan = calc_makespan(lpt_schedule, graph)

                    for g_trace_name, trace_provider in green_power_providers:
                        green_power = trace_provider()

                        create_cluster_func = lambda: create_cluster(machines_count, cores_per_machine, green_power, interval_size)

                        experiment_parameters = {
                            'graph': graph,
                            'green_power': green_power,
                            'interval_size': interval_size,
                            'power_distribution': distribution_name,
                            'cluster_factory': create_cluster_func,
                            'min_makespan': min_makespan,
                            'boundary_strategy': boundary_strategy,

                            'shift_modes': shift_modes,
                            'deadline_factors': deadline_factors,
                            'c_values': c_values,
                            'task_ordering_criterias': task_ordering_criterias,
                            'iteration': i,
                        }

                        metadata = {
                            'prefix': f'{workflow_name}_{g_trace_name}_{distribution_name}',
                            'job_number': j,
                            'job_count': job_count,
                            'experiment_count': experiment_count,
                        }

                        parameters_report = {
                            'workflow': '' + workflow_name,
                            'energy_trace': '' + g_trace_name,
                        }

                        executor.run_experiment_async(
                            lambda: experiments_per_workflow(experiment_parameters, metadata, parameters_report)
                        )
                        j += 1

            executor.wait_all()
            finished_jobs = '%.2f' % (100 * ((j-1) / float(job_count)))
            elapsed_time = seconds_to_hours(stopwatch.get_elapsed_time())
            print(f'{finished_jobs}% | {j-1} of {job_count} | {elapsed_time}')

    executor.stop()

    #import os
    #os.system("shutdown now -h")


def create_cluster(machines_count, cores_per_machine, green_power, interval_size):
    machines = []

    for i in range(machines_count):
        machine_id = f'c{cores_per_machine}_m{i}'
        machines.append(
            Machine(machine_id, cores=cores_per_machine)
        )

    power_series = PowerSeries('g1', green_power, interval_size)
    cluster = Cluster('c1', power_series, machines)

    return cluster

def simple_execution(resources_path, synthetic_path):
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    # green_power = photovoltaReader.get_trace_2(size=30)
    interval_size = 300

    deadline_factor = 2

    # graph = wfcommons_reader.read_srasearch_workflow(24, 4.26845637583, random_gauss)

    random_provider = RandomProvider(SEED, MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)
    graph = wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583,random_provider. random_gauss)
    # graph = wfcommons_reader.read_montage_workflow(1000, 11.17646556189, random_gauss)
    # graph = wfcommons_reader.read_seismology_workflow(1000, 4000, random_gauss)
    # graph = wfcommons_reader.read_blast_workflow(1000, 11.4492900609, random_gauss)
    # graph = wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, random_gauss)
    # graph = wfcommons_reader.read_cycles_workflow(1000, 31.099173553, random_gauss1)
    # graph = wfcommons_reader.read_genome_workflow(1000, 35.7812995246, random_gauss)
    # graph = wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, random_gauss)



    schedule_and_report(graph, green_power, interval_size, deadline_factor, c=0.2, show='last', shift_mode='left',
                        task_ordering='energy', print_resport=True)




if __name__ == '__main__':

    resources_path = '../../../resources'
    synthetic_path = f'{resources_path}/wfcommons/synthetic'

    MIN_TASK_POWER_DEFAULT = 1
    MAX_TASK_POWER_DEFAULT = 5
    SEED = 15735667867885

    random_provider = RandomProvider(SEED, MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)

    stopwatch = Stopwatch()
    stopwatch.start()

    # simple_execution(resources_path, synthetic_path)
    execute_experiments(resources_path, synthetic_path, random_provider)

    print(f'\n\nOverall execution: {seconds_to_hours(stopwatch.get_elapsed_time())}')
