from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsRealWorkflowReader
from src.experiments.shared.random_utils import RandomProvider

from src.scheduling.algorithms.bounded_boundary_search.bounded_boundary_search import bbs, BOUNDARY_SINGLE, \
    SHIFT_MODE_RIGHT_LEFT, TASK_SORT_ENERGY, SHIFT_MODE_LEFT, SHIFT_MODE_NONE
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.algorithms.task_flow.task_flow import task_flow_schedule
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.makespan_calculator import calc_makespan
from src.util.stopwatch import Stopwatch
from src.util.time_utils import seconds_to_hours

FILE = '../main/results_1m_deadline2_2025-05-18.txt'

def write_to_log(s):
    # write async to log file
    with open(FILE, 'a') as log_file:
        log_file.write(str(s) + '\n')
        log_file.flush()


def _create_cluster(num_of_cores, power_series):
    machine = Machine('real_traces_machine', num_of_cores)
    cluster = Cluster('real_traces_cluster', power_series, [machine])
    return cluster

def _calculate_metricts(schedule, workflow, power_series):
    calculator = EnergyUsageCalculator(power_series.green_power_list, power_series.interval_length)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(
        schedule, workflow)
    
    makespan = calc_makespan(schedule, workflow)
    dummy_task = workflow.get_first_task()
    min_start_time = float('inf')
    for child in dummy_task.successors:
        child_start_time = schedule[child.id][0]
        if child_start_time < min_start_time:
            min_start_time = child_start_time

    start_time = min_start_time - dummy_task.runtime

    DECIMAL_PLACES = 2
    return round(brown_energy_used, DECIMAL_PLACES), round(green_energy_not_used, DECIMAL_PLACES), round(total_energy, DECIMAL_PLACES), makespan, start_time


def _compare_schedulers(workflow, power_series):
    num_of_cores = len(workflow.tasks)

    cluster = _create_cluster(num_of_cores, power_series)
    lpt_schedule = lpt(workflow, [cluster])
    lpt_brown_energy_used, _, _, lpt_makespan, lpt_start_time = _calculate_metricts(lpt_schedule, workflow, power_series)
    
    
    runtime_factor = 4
    deadline = runtime_factor * lpt_makespan

    cluster = _create_cluster(num_of_cores, power_series)
    bbs_schedule_shift_right_left = bbs(workflow, deadline, 0.0, [cluster],
                                        task_sort=TASK_SORT_ENERGY,
                                        shift_mode=SHIFT_MODE_RIGHT_LEFT,
                                        boundary_strategy=BOUNDARY_SINGLE)
    bbs_right_left_brown_energy_used, _, _, bbs_right_left_makespan, bbs_right_left_start_time = _calculate_metricts(bbs_schedule_shift_right_left, workflow, power_series)

    cluster = _create_cluster(num_of_cores, power_series)
    bbs_schedule_shift_left = bbs(workflow, deadline, 0.0, [cluster],
                                  task_sort=TASK_SORT_ENERGY,
                                  shift_mode=SHIFT_MODE_LEFT,
                                  boundary_strategy=BOUNDARY_SINGLE)
    bbs_left_brown_energy_used, _, _, bbs_left_makespan, bbs_left_start_time = _calculate_metricts(bbs_schedule_shift_left, workflow, power_series)

    cluster = _create_cluster(num_of_cores, power_series)
    bbs_schedule_shift_none = bbs(workflow, deadline, 0.8, [cluster],
                                  task_sort=TASK_SORT_ENERGY,
                                  shift_mode=SHIFT_MODE_NONE,
                                  boundary_strategy=BOUNDARY_SINGLE)
    bbs_none_brown_energy_used, _, _, bbs_none_makespan, bbs_none_time = _calculate_metricts(bbs_schedule_shift_none, workflow, power_series)


    cluster = _create_cluster(num_of_cores, power_series)
    task_flow_scheduling = task_flow_schedule(workflow, [cluster])
    task_flow_brown_energy_used, _, _, task_flow_makespan, task_flow_start_time = _calculate_metricts(task_flow_scheduling, workflow, power_series)


    return {
        'lpt': {
            'brown_energy_used': lpt_brown_energy_used,
            'makespan': lpt_makespan,
            'start_time': lpt_start_time,
        },
        'bbs-shift-right-left': {
            'brown_energy_used': bbs_right_left_brown_energy_used,
            'makespan': bbs_right_left_makespan,
            'start_time': bbs_right_left_start_time,
        },
        'bbs-shift-left': {
            'brown_energy_used': bbs_left_brown_energy_used,
            'makespan': bbs_left_makespan,
            'start_time': bbs_left_start_time,       
        },
        'bbs-shift-none': {
            'brown_energy_used': bbs_none_brown_energy_used,
            'makespan': bbs_none_makespan,
            'start_time': bbs_none_time,       
        },
        'task_flow': {
            'brown_energy_used': task_flow_brown_energy_used,
            'makespan': task_flow_makespan,
            'start_time': task_flow_start_time,       
        }
    }

def print_results_headers():
    s = '\t'.join(['workflow', 'lpt_brown_energy_used', 'lpt_makespan', 'lpt_start_time',
                   'bbs_right_left_brown_energy_used', 'bbs_right_left_makespan', 'bbs_right_left_start_time',
                   'bbs_left_brown_energy_used', 'bbs_left_makespan', 'bbs_left_start_time',
                   'bbs_none_brown_energy_used', 'bbs_none_makespan', 'bbs_none_start_time',
                   'task_flow_brown_energy_used', 'task_flow_makespan', 'task_flow_start_time'])
    print(s)
    write_to_log(s)

def print_results(workflow_name, workflow_results):
    s = '\t'.join([
        workflow_name,
        str(workflow_results['lpt']['brown_energy_used']),
        str(workflow_results['lpt']['makespan']),
        str(workflow_results['lpt']['start_time']),
        str(workflow_results['bbs-shift-right-left']['brown_energy_used']),
        str(workflow_results['bbs-shift-right-left']['makespan']),
        str(workflow_results['bbs-shift-right-left']['start_time']),
        str(workflow_results['bbs-shift-left']['brown_energy_used']),
        str(workflow_results['bbs-shift-left']['makespan']),
        str(workflow_results['bbs-shift-left']['start_time']),
        str(workflow_results['bbs-shift-none']['brown_energy_used']),
        str(workflow_results['bbs-shift-none']['makespan']),
        str(workflow_results['bbs-shift-none']['start_time']),
        str(workflow_results['task_flow']['brown_energy_used']),
        str(workflow_results['task_flow']['makespan']),
        str(workflow_results['task_flow']['start_time'])
    ])
    print(s)
    write_to_log(s)

def execute_experiment(resources_path, random_provider):
    real_traces_reader = WfCommonsRealWorkflowReader(resources_path + '/wfcommons/real_traces')
    photovolta_reader = PhotovoltaReader(resources_path)

    green_power_providers = [
        ('trace-1', lambda: photovolta_reader.get_trace_1(size=30)),
        ('trace-2', lambda: photovolta_reader.get_trace_2(size=30)),
        ('trace-3', lambda: photovolta_reader.get_trace_2(size=30)),
        ('trace-4', lambda: photovolta_reader.get_trace_2(size=30)),
    ]

    workflows_providers = [
        ('blast', lambda: real_traces_reader.get_blast(random_provider.random_uniform)),
        ('bwa', lambda: real_traces_reader.get_bwa(random_provider.random_uniform)),
        ('cycles', lambda: real_traces_reader.get_cycles(random_provider.random_uniform)),
        ('genome', lambda: real_traces_reader.get_genome(random_provider.random_uniform)),
        ('soykb', lambda: real_traces_reader.get_soykb(random_provider.random_uniform)),
        ('srasearch', lambda: real_traces_reader.get_srasearch(random_provider.random_uniform)),
        ('montage', lambda: real_traces_reader.get_montage(random_provider.random_uniform)),
        ('seismology', lambda: real_traces_reader.get_seismology(random_provider.random_uniform)),
    ]

    start_times = [
        0,
        #3 * 3600,  # 3h
        6 * 3600, # 6h
        #9 * 3600, # 9h
        12 * 3600, # 12h
        #15 * 3600, # 15h
        18 * 3600, #18h
    ]

    interval_length = 300 # 5 minutes

    for start_time in start_times:

        green_power_list = photovolta_reader.get_trace_2(size=1)

        # Adjust start time
        intervals_to_remove = start_time / interval_length
        green_power_list = green_power_list[int(intervals_to_remove):]

        power_series = PowerSeries('real_power_series', green_power_list,interval_length)

        print('-----------------------')
        write_to_log('-----------------------')

        print('Start time: ', start_time)
        write_to_log(f'Start time: {start_time}')

        print_results_headers()

        for workflow_name, workflow_provider in workflows_providers:

            workflow = workflow_provider()

            results = _compare_schedulers(workflow, power_series)
            print_results(workflow_name, results)

def _get_workflow_statics(resources_path):
    real_traces_reader = WfCommonsRealWorkflowReader(resources_path + '/wfcommons/real_traces')
    workflows_providers = [
        ('blast', lambda: real_traces_reader.get_blast(lambda: 1)),
        ('bwa', lambda: real_traces_reader.get_bwa(lambda: 1)),
        ('cycles', lambda: real_traces_reader.get_cycles(lambda: 1)),
        ('genome', lambda: real_traces_reader.get_genome(lambda: 1)),
        ('soykb', lambda: real_traces_reader.get_soykb(lambda: 1)),
        ('srasearch', lambda: real_traces_reader.get_srasearch(lambda: 1)),
        ('montage', lambda: real_traces_reader.get_montage(lambda: 1)),
        ('seismology', lambda: real_traces_reader.get_seismology(lambda: 1)),
    ]

    for workflow_name, workflow_provider in workflows_providers:
        workflow = workflow_provider()
        print(f'{workflow_name}\t{len(workflow.tasks)}')

if __name__ == '__main__':
    # Clear log file at start
    with open(FILE, 'w') as log_file:
        log_file.write('')

    real_traces_path = '../../../resources'

    _get_workflow_statics(real_traces_path)

    MIN_TASK_POWER_DEFAULT = 1
    MAX_TASK_POWER_DEFAULT = 5
    SEED = 15735667867885


    random_provider = RandomProvider(SEED, MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)

    stopwatch = Stopwatch()
    stopwatch.start()

    execute_experiment(real_traces_path, random_provider)

    print(f'\n\nOverall execution: {seconds_to_hours(stopwatch.get_elapsed_time())}')
    write_to_log(f'\n\nOverall execution: {seconds_to_hours(stopwatch.get_elapsed_time())}')
