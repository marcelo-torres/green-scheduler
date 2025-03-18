import csv
import itertools
import random
import statistics

from src.data.photovolta import PhotovoltaReader
from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine_factory import create_machines_with_target
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.makespan_estimator import estimate_min_makespan
from src.scheduling.util.scheduling_check import check
from tests.scheduling.algorithms.highest_power_first.highest_power_first_sanity_test_data import load_workflows

MIN_POWER = 5
MAX_POWER = 50

def run_experiments():
    file_full_path = '../test/compare_boundary_approach_2025-03-16_5.csv'
    headers = ['workflow', 'shift_mode', 'deadline_factor', 'c-value', 'task_sort', 'use_lpt_boundary', 'makespan',
               'brown_energy_used']
    data_reports = []

    create_csv_file(file_full_path, headers)

    workflow_providers = load_workflows()
    #workflow_providers = filter(lambda w: w[0] in ['soykb'], workflow_providers)

    use_lpt_boundary_parameters = [False, True]

    shift_modes = ['none', 'left', 'right-left']
    deadline_factors = [1, 2]
    c_values = [0, 0.5]
    task_ordering_criterias = ['energy']



    for workflow_provider, use_lpt_boundary in itertools.product(workflow_providers, use_lpt_boundary_parameters):

        list_of_makespan = []
        list_of_brown_energy_used = []

        print(f'{workflow_provider[0]} with use_lpt_boundary={use_lpt_boundary}')

        for shift, d, c, ordering in itertools.product(shift_modes, deadline_factors, c_values,
                                                                          task_ordering_criterias):
            random.seed(15735667867885)
            uniform = lambda: random.uniform(MIN_POWER, MAX_POWER)

            graph = workflow_provider[1](uniform)
            clusters, deadline = _create_clusters(graph, d)
            schedule = highest_power_first(graph, deadline, c, clusters, task_sort=ordering, shift_mode=shift, use_lpt_boundary=use_lpt_boundary)

            violations = check(schedule, graph)
            assert len(violations) == 0

            power_series = clusters[0].power_series
            energy_calculator = EnergyUsageCalculator(power_series.green_power_list, power_series.interval_length)
            brown_energy_used, green_energy_not_used, total_energy = energy_calculator.calculate_energy_usage_for_scheduling(
                schedule, graph)
            green_energy_used = total_energy - brown_energy_used

            makespan = calc_makespan(schedule, graph)

            list_of_makespan.append(makespan)
            list_of_brown_energy_used.append(brown_energy_used)

            data_reports.append({
                'workflow': workflow_provider[0],
                'shift_mode': shift,
                'deadline_factor': d,
                'c-value': c,
                'task_sort': ordering,
                'use_lpt_boundary': use_lpt_boundary,
                'makespan': makespan,
                'brown_energy_used': brown_energy_used
            })

        # print(f'makespan: {makespan}')
        # print(f'brown_energy_used: {brown_energy_used}')

        def report(name, data):
            mean = statistics.mean(data)
            stdev = statistics.stdev(data)
            print(f'{name}: {round(mean, 2)}s. std: {round(stdev, 2)}')

        report('makespan', list_of_makespan)
        report('brown_energy_used', list_of_brown_energy_used)
        print('')
        if use_lpt_boundary:
            print('\n---------------------------------------')

    write_reports_to_csv(data_reports, headers, file_full_path)

def _create_clusters(graph, deadline_factor):
    critical_path_length = calc_critical_path_length(graph)
    #machines = create_machines_with_target(graph, critical_path_length * deadline_factor, [64], 0.40)
    machines = create_machines_with_target(graph, critical_path_length * deadline_factor, [64], 0.40)

    min_makespan = estimate_min_makespan(graph, machines)
    deadline = min_makespan * deadline_factor

    #machines = create_machines_with_target(graph, deadline, [124], 0.40)
    machines = create_machines_with_target(graph, deadline, [64], 0.40)

    # intervals = math.ceil(critical_path_length / 10)
    # green_power_list = [random.uniform(MIN_POWER-1, MAX_POWER) for i in range(intervals)]

    resources_path = '../../../resources'
    photovolta_reader = PhotovoltaReader(resources_path)
    green_power_list = photovolta_reader.get_trace_1(size=10)

    power_series = PowerSeries('g1', green_power_list, 300)

    clusters = [Cluster('c1', power_series, machines)]

    return clusters, deadline

def create_csv_file(file_path_and_name, headers):
    with open(file_path_and_name, 'x') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)

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

if __name__ == '__main__':
    run_experiments()