import csv
import random
from datetime import datetime, timedelta
import os

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.experiments.ParallelExperimentExecutor import ParallelExperimentExecutor
from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import create_single_machine_cluster
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check
from src.scheduling.util.stretch_calculator import calc_stretch
from src.util.stopwatch import Stopwatch

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

MIN_TASK_POWER_DEFAULT = 1
MAX_TASK_POWER_DEFAULT = 5

random.seed(15735667867885)
def random_uniform():
    return random.uniform(MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)


def random_gauss():
    mu = 3  # Mean
    sigma = 0.9  # Standard deviation

    value = random.gauss(mu, sigma)
    while value < MIN_TASK_POWER_DEFAULT or value > MAX_TASK_POWER_DEFAULT:
        value = random.gauss(mu, sigma)
    return value


def random_expovariate():
    lambd = 1
    value = random.expovariate(lambd)
    while value < MIN_TASK_POWER_DEFAULT or value > MAX_TASK_POWER_DEFAULT:
        value = random.expovariate(lambd)
    return value


def random_expovariate_inverse():
    return MAX_TASK_POWER_DEFAULT - random_expovariate()


def seconds_to_hours(seconds):
    return str(
        timedelta(seconds=seconds)
    )


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


def schedule_and_report(graph, green_power, interval_size, deadline_factor, task_ordering, shift_mode='right-left',
                        c=0.0, show='off', print_resport=False):
    min_makespan = calc_critical_path_length(graph)
    deadline = min_makespan * deadline_factor
    number_of_tasks = len(graph.tasks)

    if print_resport:
        print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')
        print(f'\tDeadline: {deadline:,}s ({seconds_to_hours(deadline)})')
        print(f'\tNumber of tasks: {number_of_tasks:,.{2}f}')

    cluster = create_single_machine_cluster(green_power, interval_size, cores=5000)
    scheduling = highest_power_first(graph, deadline, c, [cluster], task_sort=task_ordering, shift_mode=shift_mode, show=show)

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

    shift_modes = experiment_parameters['shift_modes']
    deadline_factors = experiment_parameters['deadline_factors']
    c_values = experiment_parameters['c_values']
    task_ordering_criterias = experiment_parameters['task_ordering_criterias']

    prefix = metadata['prefix']
    job_number = metadata['job_number']
    job_count = metadata['job_count']
    experiment_count = metadata['experiment_count']

    print(f'JOB {job_number} of {job_count} starting | Iteration {iteration} | {prefix}')

    reports = []
    i = 1
    for shift_mode in shift_modes:
        for deadline_factor in deadline_factors:
            for c_value in c_values:
                for task_ordering in task_ordering_criterias:
                    parameters_report_temp = parameters_report | {
                        'experiment': f'J{job_number}_E{i}',
                        'experiment_type': f'J{prefix}',
                        'iteration': f'{iteration}',
                        'shift_mode': shift_mode,
                        'deadline_factor': deadline_factor,
                        'c_value': c_value,
                        'task_ordering': task_ordering,
                        'power_distribution': power_distribution,
                        'job_number': job_number
                    }

                    report = schedule_and_report(graph, green_power, interval_size, deadline_factor, task_ordering,
                                                 c=c_value, shift_mode=shift_mode)

                    full_report = parameters_report_temp | report
                    reports.append(full_report)

                    i += 1

    print(f'JOB {job_number} finished')
    return reports


def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_experiment_id(dt):
    return dt.strftime("experiments_%Y-%m-%d_%H-%M-%S")


def write_reports_to_csv(reports, headers, file_full_path):

    with open(file_full_path, 'a') as csvfile:
        csvwriter = csv.writer(csvfile)

        for report in reports:
            row = []
            for header in headers:
                row.append(
                    report[header]
                )
            csvwriter.writerow(row)


def create_csv_file(start_time, headers):
    experiment_id = get_experiment_id(start_time)
    experiments_reports_path = resources_path + f'/experiments-shift/{experiment_id}'
    file_full_path = f'{experiments_reports_path}/report_{experiment_id}.csv'

    create_dir(experiments_reports_path)
    with open(file_full_path, 'x') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)

    return file_full_path


def execute_experiments():

    headers = [
        'job_number', 'experiment', 'experiment_type', 'iteration', 'workflow', 'energy_trace',
        'shift_mode', 'c_value', 'deadline_factor', 'deadline',
        'task_ordering', 'scheduling_hash', 'power_distribution',
        'min_makespan', 'makespan', 'workflow_stretch',
        'brown_energy_used', 'green_energy_used', 'total_energy', 'green_energy_not_used',
        'max_active_tasks', 'active_tasks_mean', 'active_tasks_std', 'number_of_tasks',
        'scheduling_violations',
    ]

    start_time = datetime.now()
    file_full_path = create_csv_file(start_time, headers)

    def save_report(report):
        return write_reports_to_csv(report, headers, file_full_path)
    executor = ParallelExperimentExecutor(save_report)


    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovolta_reader = PhotovoltaReader(resources_path)
    interval_size = 300

    random_functions = [
        ('uniform', random_uniform),
        ('gaussian', random_gauss),
        ('inverted_exponential', random_expovariate_inverse),
    ]

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

    green_power_providers = [
        ('trace-1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace-2', lambda: photovolta_reader.get_trace_2(size=30)),
        ('trace-3', lambda: photovolta_reader.get_trace_2(size=30)),
        ('trace-4', lambda: photovolta_reader.get_trace_2(size=30)),
    ]

    # Process parameters
    experiment_repetitions = 10
    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [2, 4, 8]
    c_values = [0, 0.5, 0.8]
    task_ordering_criterias = ['energy', 'power', 'runtime', 'runtime_ascending']

    # shift_modes = ['none']
    # deadline_factors = [2]
    # c_values = [0]
    # task_ordering_criterias = ['energy']

    job_count = len(random_functions) * len(workflow_providers) * len(green_power_providers) * experiment_repetitions
    experiment_count = len(shift_modes) * len(deadline_factors) * len(c_values) * len(task_ordering_criterias)

    print(f'{experiment_count} experiments will be executed in {job_count} jobs. Total: {job_count * experiment_count}\n')

    executor.start()

    stopwatch = Stopwatch()
    stopwatch.start()

    j = 1
    for distribution_name, random_function in random_functions:
        for workflow_name, workflow_provider in workflow_providers:
            for i in range(experiment_repetitions):
                graph = workflow_provider(random_function)

                for g_trace_name, trace_provider in green_power_providers:
                    green_power = trace_provider()

                    experiment_parameters = {
                        'graph': graph,
                        'green_power': green_power,
                        'interval_size': interval_size,
                        'power_distribution': distribution_name,

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


def simple_execution():
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    #green_power = photovoltaReader.get_trace_2(size=30)
    interval_size = 300

    deadline_factor = 2

    #graph = wfcommons_reader.read_srasearch_workflow(24, 4.26845637583, random_gauss)

    graph = wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583, random_gauss)
    #graph = wfcommons_reader.read_montage_workflow(1000, 11.17646556189, random_gauss)
    #graph = wfcommons_reader.read_seismology_workflow(1000, 4000, random_gauss)
    #graph = wfcommons_reader.read_blast_workflow(1000, 11.4492900609, random_gauss)
    #graph = wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, random_gauss)
    #graph = wfcommons_reader.read_cycles_workflow(1000, 31.099173553, random_gauss1)
    #graph = wfcommons_reader.read_genome_workflow(1000, 35.7812995246, random_gauss)
    #graph = wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, random_gauss)



    schedule_and_report(graph, green_power, interval_size, deadline_factor, c=0.2, show='last', shift_mode='left',
                        task_ordering='energy', print_resport=True)

def calc_min_makespans_of_workflows():
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)

    workflows = [
        ('blast', lambda random_power: wfcommons_reader.read_blast_workflow(1000, 11.4492900609, random_power)),
        ('bwa', lambda random_power: wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, random_power)),
        ('cycles', lambda random_power: wfcommons_reader.read_cycles_workflow(1000, 31.0991735531, random_power)),
        ('genome', lambda random_power: wfcommons_reader.read_genome_workflow(1000, 35.7812995246, random_power)),
        ('soykb', lambda random_power: wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, random_power)),
        ('srasearch', lambda random_power: wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583, random_power)),
        ('montage', lambda random_power: wfcommons_reader.read_montage_workflow(1000, 11.17646556189, random_power)),
        ('seismology', lambda random_power: wfcommons_reader.read_seismology_workflow(1000, 4000, random_power)),
    ]

    for name, workflow_provider in workflows:
        workflow = workflow_provider(random_gauss)
        min_makespan = calc_critical_path_length(workflow)
        #print(f'\t{name}\tmin_makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})\t tasks: {len(workflow.list_of_tasks())}')
        print(
            f'{min_makespan}')

if __name__ == '__main__':
    stopwatch = Stopwatch()
    stopwatch.start()

    calc_min_makespans_of_workflows()
    #simple_execution()
    #execute_experiments()

    print(f'\n\nOverall execution: ', seconds_to_hours(stopwatch.get_elapsed_time()))
