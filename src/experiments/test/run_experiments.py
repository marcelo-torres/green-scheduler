import csv
from datetime import datetime, timedelta
import os

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.experiments.ParallelExperimentExecutor import ParallelExperimentExecutor
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check
from src.util.stopwatch import Stopwatch

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

MIN_TASK_POWER_DEFAULT = 1
MAX_TASK_POWER_DEFAULT = 5


def seconds_to_hours(seconds):
    return str(
        timedelta(seconds=seconds)
    )


def report_scheduling(scheduling, graph, energy_calculator):
    makespan = calc_makespan(scheduling, graph)
    workflow_start_time = scheduling[graph.get_first_task().id]  #TODO fix this
    workflow_stretch = makespan - workflow_start_time
    print(f'\tMakespan: {makespan:,}s ({seconds_to_hours(makespan)})')
    print(f'\tWorkflow Stretch: {workflow_stretch:,}s ({seconds_to_hours(workflow_stretch)})')
    print()

    scheduling_hash = hash(frozenset(scheduling.items()))
    print(f'\tscheduling_hash: {scheduling_hash:}')
    print()

    brown_energy_used, green_energy_not_used, total_energy = energy_calculator.calculate_energy_usage_for_scheduling(
        scheduling, graph)
    green_energy_used = total_energy - brown_energy_used
    print(f'\tBrown energy used: {brown_energy_used:,.{2}f}J')
    print(f'\t...Green energy used: {green_energy_used:,.{2}f}J')
    print(f'\t...Total energy used: {total_energy:,.{2}f}J')
    print(f'\tGreen energy not used: {green_energy_not_used:,.{2}f}J')
    print()

    max_active_tasks, mean, std, active_tasks_by_time = count_active_tasks(scheduling, graph)
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
                        c=0.0, show='off'):
    min_makespan = calc_critical_path_length(graph)
    deadline = min_makespan * deadline_factor
    number_of_tasks = len(graph.tasks)

    print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')
    print(f'\tDeadline: {deadline:,}s ({seconds_to_hours(deadline)})')
    print(f'\tNumber of tasks: {number_of_tasks:,.{2}f}')

    scheduling = schedule_graph(graph, deadline, green_power, interval_size, c=c, show=show, shift_mode=shift_mode,
                                task_ordering=task_ordering)

    energy_calculator = EnergyUsageCalculator(green_power, interval_size)
    scheduling_report = report_scheduling(scheduling, graph, energy_calculator)

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

    print('\tViolations: ', check(scheduling, graph))
    print()

    return full_report


def experiments_per_workflow(experiment_parameters, metadata, parameters_report):

    graph = experiment_parameters['graph']
    green_power = experiment_parameters['green_power']
    interval_size = experiment_parameters['interval_size']

    shift_modes = experiment_parameters['shift_modes']
    deadline_factors = experiment_parameters['deadline_factors']
    c_values = experiment_parameters['c_values']
    task_ordering_criterias = experiment_parameters['task_ordering_criterias']

    prefix = metadata['prefix']
    job_number = metadata['job_number']
    job_count = metadata['job_count']
    experiment_count = metadata['experiment_count']

    reports = []
    i = 1
    for shift_mode in shift_modes:
        for deadline_factor in deadline_factors:
            for c_value in c_values:
                for task_ordering in task_ordering_criterias:
                    parameters_report_temp = parameters_report | {
                        'experiment': f'J{job_number}_E{i}',
                        'shift_mode': shift_mode,
                        'deadline_factor': deadline_factor,
                        'c_value': c_value,
                        'task_ordering': task_ordering
                    }

                    print(f'JOB {job_number} of {job_count} | Experiment {i} of {experiment_count} | {prefix}')

                    report = schedule_and_report(graph, green_power, interval_size, deadline_factor, task_ordering,
                                                 c=c_value, shift_mode=shift_mode)

                    full_report = parameters_report_temp | report
                    reports.append(full_report)

                    i += 1
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
        'experiment', 'workflow', 'energy_trace', 'shift_mode', 'c_value', 'deadline_factor', 'deadline',
        'task_ordering', 'scheduling_hash',
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

    workflow_providers = [
        ('blast', lambda: wfcommons_reader.read_blast_workflow(1000, 11.4492900609)),
        ('bwa', lambda: wfcommons_reader.read_bwa_workflow(1000, 52.2248138958)),
        ('cycles', lambda: wfcommons_reader.read_cycles_workflow(1000, 31.0991735531)),
        ('genome', lambda: wfcommons_reader.read_genome_workflow(1000, 35.7812995246)),
        ('soykb', lambda: wfcommons_reader.read_soykb_workflow(1000, 3.85224364443)),
        ('srasearch', lambda: wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583)),
        ('montage', lambda: wfcommons_reader.read_montage_workflow(1000, 11.17646556189)),
        ('seismology', lambda: wfcommons_reader.read_seismology_workflow(1000, 4000)),
    ]

    green_power_providers = [
        ('trace-1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace-2', lambda: photovolta_reader.get_trace_2(size=30))
    ]

    # Process parameters
    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [1, 2, 4, 8]
    c_values = [0, 0.2, 0.5, 0.8]
    task_ordering_criterias = ['energy', 'power', 'runtime', 'runtime_ascending']

    job_count = len(workflow_providers) * len(green_power_providers)
    experiment_count = len(shift_modes) * len(deadline_factors) * len(c_values) * len(task_ordering_criterias)

    print(f'{experiment_count} experiments will be executed.')

    executor.start()

    j = 1
    for workflow_name, workflow_provider in workflow_providers:
        graph = workflow_provider()

        for g_trace_name, trace_provider in green_power_providers:
            green_power = trace_provider()

            experiment_parameters = {
                'graph': graph,
                'green_power': green_power,
                'interval_size': interval_size,

                'shift_modes': shift_modes,
                'deadline_factors': deadline_factors,
                'c_values': c_values,
                'task_ordering_criterias': task_ordering_criterias,
            }

            metadata = {
                'prefix': f'{workflow_name}_{g_trace_name}',
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

    #import os
    #os.system("shutdown now -h")


def simple_execution():
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    #green_power = photovoltaReader.get_trace_2(size=30)
    interval_size = 300

    deadline_factor = 2

    #graph = wfcommons_reader.read_srasearch_workflow(24, 4.26845637583)

    graph = wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583)
    #graph = wfcommons_reader.read_montage_workflow(1000, 11.17646556189)
    #graph = wfcommons_reader.read_seismology_workflow(1000, 4000)
    #graph = wfcommons_reader.read_blast_workflow(1000, 11.4492900609)
    #graph = wfcommons_reader.read_bwa_workflow(1000, 52.2248138958)
    #graph = wfcommons_reader.read_cycles_workflow(1000, 31.0991735531)
    #graph = wfcommons_reader.read_genome_workflow(1000, 35.7812995246 )
    #graph = wfcommons_reader.read_soykb_workflow(1000, 3.85224364443)

    min_makespan = calc_critical_path_length(graph)
    print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')

    schedule_and_report(graph, green_power, interval_size, deadline_factor, c=0.2, show='last', shift_mode='left',
                        task_ordering='energy')


if __name__ == '__main__':
    stopwatch = Stopwatch()
    stopwatch.start()

    #simple_execution()
    execute_experiments()

    print(f'\n\nOverall execution: ', seconds_to_hours(stopwatch.get_elapsed_time()))