import csv
from contextlib import redirect_stdout
from datetime import datetime, timedelta
import os

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
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
    workflow_start_time = scheduling[graph.get_first_task().id] #TODO fix this
    workflow_stretch = makespan - workflow_start_time
    print(f'\tMakespan: {makespan:,}s ({seconds_to_hours(makespan)})')
    print(f'\tWorkflow Stretch: {workflow_stretch:,}s ({seconds_to_hours(workflow_stretch)})')
    print()

    scheduling_hash = hash(frozenset(scheduling.items()))
    print(f'\tscheduling_hash: {scheduling_hash:}')
    print()

    brown_energy_used, green_energy_not_used, total_energy = energy_calculator.calculate_energy_usage_for_scheduling(scheduling, graph)
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


def schedule_and_report(graph, green_power, interval_size, deadline_factor, task_ordering, shift_mode='right-left', c=0.0, show='off', id=0, total=0):

    min_makespan = calc_critical_path_length(graph)
    deadline = min_makespan * deadline_factor
    number_of_tasks = len(graph.tasks)

    print(f'Execution {id}/{total}')

    print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')
    print(f'\tDeadline: {deadline:,}s ({seconds_to_hours(deadline)})')
    print(f'\tNumber of tasks: {number_of_tasks:,.{2}f}')
    print()

    scheduling = schedule_graph(graph, deadline, green_power, interval_size, c=c, show=show, shift_mode=shift_mode, task_ordering=task_ordering)

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

    print(
        'Violations: ', check(scheduling, graph)
    )

    return full_report




def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_experiment_id(dt):
    return dt.strftime("experiments_%Y-%m-%d_%H-%M-%S")


def execute_experiments():
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
        ('trace_1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace_2', lambda: photovolta_reader.get_trace_2(size=30))
    ]

    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [1, 2, 4, 8]

    c_values = [0, 0.2, 0.5, 0.8]

    task_ordering_criterias = ['energy', 'power', 'runtime']

    experiments_count = len(workflow_providers) * len(green_power_providers) * len(shift_modes) * len(deadline_factors) * len(c_values) * len(task_ordering_criterias)

    headers = {
        'experiment', 'workflow', 'energy_trace', 'shift_mode', 'deadline_factor', 'c_value', 'scheduling_hash',
        
        'makespan',
        'workflow_stretch',
        'brown_energy_used',
        'green_energy_used',
        'total_energy',
        'green_energy_not_used',
        'max_active_tasks',
        'active_tasks_mean',
        'active_tasks_std',
        'min_makespan',
        'deadline',
        'number_of_tasks',
        'task_ordering',
        'scheduling_violations',
    }

    start_time = datetime.now()

    experiment_id = get_experiment_id(start_time)
    experiments_reports_path = resources_path + f'/experiments-shift/{experiment_id}'
    file_full_path = f'{experiments_reports_path}/report_{experiment_id}.csv'
    log_file_full_path = f'{experiments_reports_path}/logs_{experiment_id}.txt'

    create_dir(experiments_reports_path)

    with open(log_file_full_path, 'w') as f:
        with redirect_stdout(f):
            with open(file_full_path, 'x') as csvfile:

                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(headers)

                i = 1
                for workflow_name, workflow_provider in workflow_providers:
                    graph = workflow_provider()

                    for g_trace_name, trace_provider in green_power_providers:
                        green_power = trace_provider()

                        for shift_mode in shift_modes:
                            for deadline_factor in deadline_factors:
                                for c_value in c_values:
                                    for task_ordering in task_ordering_criterias:
                                        parameters = {
                                            'experiment': i,
                                            'workflow': workflow_name,
                                            'energy_trace': g_trace_name,
                                            'shift_mode': shift_mode,
                                            'deadline_factor': deadline_factor,
                                            'c_value': c_value,
                                            'task_ordering': task_ordering
                                        }

                                        report = schedule_and_report(graph, green_power, interval_size, deadline_factor, task_ordering, c=c_value, shift_mode=shift_mode, id=i, total=experiments_count)

                                        full_report = parameters | report

                                        row = []
                                        for header in headers:
                                            row.append(
                                                full_report[header]
                                            )
                                        csvwriter.writerow(row)
                                        i += 1
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

    schedule_and_report(graph, green_power, interval_size, deadline_factor, c=0.2, show='last', shift_mode='left', task_ordering='energy')


    
if __name__ == '__main__':
    stopwatch = Stopwatch()
    stopwatch.start()

    #simple_execution()
    execute_experiments()

    print(f'Overall execution: %.4fs' % stopwatch.get_elapsed_time()) # TODO format as datetime

    pass
