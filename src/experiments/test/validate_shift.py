import csv
from contextlib import redirect_stdout
from datetime import datetime, timedelta
import os
import pathlib

from wfcommons import WorkflowGenerator, SrasearchRecipe, MontageRecipe, SeismologyRecipe, BlastRecipe, BwaRecipe, \
    CyclesRecipe, GenomeRecipe, SoykbRecipe

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons import _create_graph
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

MIN_TASK_POWER_DEFAULT = 1
MAX_TASK_POWER_DEFAULT = 5

def _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power):
    generator = WorkflowGenerator(recipe)

    if not os.path.isfile(workflow_name):
        for i, workflow in enumerate(generator.build_workflows(1)):
            workflow.write_json(pathlib.Path(workflow_name))
    temp_graph = _create_graph(workflow_name, min_task_power, max_task_power)

    return temp_graph


def get_srasearch_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/srasearch-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = SrasearchRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_montage_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/montage-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = MontageRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_seismology_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/seismology-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = SeismologyRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_blast_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/blast-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = BlastRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_bwa_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/bwa-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = BwaRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_cycles_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/cycles-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = CyclesRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_genome_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/genome-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = GenomeRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


def get_soykb_workflow(num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
    workflow_name = f'{synthetic_path}/soykb-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    recipe = SoykbRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
    return _create_wfcommons_graph(workflow_name, recipe, min_task_power, max_task_power)


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

    full_report = pre_scheduling_report | scheduling_report

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

    photovolta_reader = PhotovoltaReader(resources_path)
    interval_size = 300

    workflow_providers = [
        ('blast', lambda: get_blast_workflow(1000, 11.4492900609)),
        ('bwa', lambda: get_bwa_workflow(1000, 52.2248138958)),
        ('cycles', lambda: get_cycles_workflow(1000, 31.0991735531)),
        ('genome', lambda: get_genome_workflow(1000, 35.7812995246)),
        ('soykb', lambda: get_soykb_workflow(1000, 3.85224364443)),
        ('srasearch', lambda: get_srasearch_workflow(1000, 1.26845637583)),
        ('montage', lambda: get_montage_workflow(1000, 11.17646556189)),
        ('seismology', lambda: get_seismology_workflow(1000, 4000)),
    ]

    green_power_providers = [
        ('trace_1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace_2', lambda: photovolta_reader.get_trace_2(size=30))
    ]

    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [2, 4, 8]

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
        'task_ordering'
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
    import os
    os.system("shutdown now -h")


def simple_execution():

    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    #green_power = photovoltaReader.get_trace_2(size=30)
    interval_size = 300

    deadline_factor = 2

    #graph = get_srasearch_workflow(24, 4.26845637583)

    graph = get_srasearch_workflow(1000, 1.26845637583)
    #graph = get_montage_workflow(1000, 11.17646556189)
    #graph = get_seismology_workflow(1000, 4000)
    #graph = get_blast_workflow(1000, 11.4492900609)
    #graph = get_bwa_workflow(1000, 52.2248138958)
    #graph = get_cycles_workflow(1000, 31.0991735531)
    #graph = get_genome_workflow(1000, 35.7812995246 )
    #graph = get_soykb_workflow(1000, 3.85224364443)

    min_makespan = calc_critical_path_length(graph)
    print(f'\tMin makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})')

    schedule_and_report(graph, green_power, interval_size, deadline_factor, c=0.2, show='last', shift_mode='left', task_ordering='energy')


    
if __name__ == '__main__':
    #simple_execution()
    execute_experiments()

    pass
