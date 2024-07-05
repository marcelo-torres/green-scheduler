import csv
import os
from datetime import datetime

from src.data.photovolta import PhotovoltaReader
from src.data.workflow_parquet_reader import WorkflowTraceArchiveReader
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check
from src.util.stopwatch import Stopwatch


def report(s):
    print(s)


def format_date(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_experiment_id(dt):
    return dt.strftime("experiments_%Y-%m-%d_%H-%M-%S")


def run_highest_power_first(graph, deadline, green_power, interval_size, c, max_green_power, figure_file, task_ordering_criteria):
    scheduling = schedule_graph(graph, deadline, green_power, interval_size, c=c, show='off', max_power=max_green_power, figure_file=figure_file, task_ordering=task_ordering_criteria)

    # Makespan Report
    makespan = calc_makespan(scheduling, graph)
    report(f'\tmakespan: %.4fs' % makespan)

    # Energy Report
    calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(
        scheduling)

    report(f'\tbrown_energy_used: %.4fJ' % brown_energy_used)
    report(f'\tgreen_energy_not_used: %.4fJ' % brown_energy_used)
    report(f'\ttotal_energy: %.4fJ' % brown_energy_used)

    # Scheduling Violation Report
    scheduling_violations = check(scheduling, graph)
    if len(scheduling_violations) > 0:
        report(f'\n {len(scheduling_violations)} scheduling violations were found')
        for task_id, start, pred_id, pred_finish_time in scheduling_violations:
            report(f'Task {task_id} start: {start} | Pred {pred_id} finish time: {pred_finish_time}')

    return makespan, brown_energy_used, green_energy_not_used, total_energy

def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == '__main__':

    resources_path = './../resources'
    min_task_power = 20
    max_task_power = 100

    reader = WorkflowTraceArchiveReader(resources_path, min_task_power, max_task_power)
    photovoltaReader = PhotovoltaReader(resources_path)

    interval_size = 10
    max_green_power = 1000

    graph_providers = [
        ('epigenomics', reader.epigenomics),
        ('montage', reader.montage)
    ]
    green_power_providers = [
        ('trace_1', photovoltaReader.get_trace_1),
        ('trace_2', photovoltaReader.get_trace_2)
    ]

    task_ordering_criterias = ['energy', 'power', 'runtime']

    deadline_factors = [1, 2, 4, 8, 10]
    c_values = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]

    experiments_count = (len(graph_providers) * len(green_power_providers) * len(deadline_factors) * len(c_values)
                         * len(task_ordering_criterias))
    start_time = datetime.now()
    report(f'[{format_date(start_time)}] Starting {experiments_count} experiments...\n')

    experiment_id = get_experiment_id(start_time)
    experiments_reports_path = resources_path + f'/experiments/{experiment_id}'
    experiments_figures_path = resources_path + f'/experiments/{experiment_id}/figures'
    file_full_path = f'{experiments_reports_path}/report_{experiment_id}.csv'

    create_dir(experiments_reports_path)
    create_dir(experiments_figures_path)

    with open(file_full_path, 'x') as csvfile:

        stopwatch = Stopwatch()
        stopwatch.start()

        headers = ['experiment', 'workflow', 'energy_trace', 'algorithm', 'task_ordering', 'deadline',
                   'deadline_factor', 'c', 'min_makespan', 'makespan', 'brown_energy_used', 'green_energy_not_used',
                   'total_energy']
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)

        i = 1
        for graph_name, graph_provider in graph_providers:
            for g_power_trace_name, green_power_provider in green_power_providers:
                for task_ordering_criteria in task_ordering_criterias:
                    for deadline_factor in deadline_factors:
                        for c in c_values:
                            report(
                                f'{i}/{experiments_count}: {graph_name} {g_power_trace_name} task_ordering={task_ordering_criteria} deadline_factor={deadline_factor} c={c}')

                            graph = graph_provider()
                            green_power = green_power_provider()

                            min_makespan = calc_critical_path_length(graph)
                            deadline = deadline_factor * min_makespan
                            report(f'\tmin_makespan: {min_makespan}s')

                            try:
                                #draw_task_graph(graph)
                                figure_file = f'{experiments_figures_path}/{i}_scheduling_figure.png'
                                makespan, brown_energy_used, green_energy_not_used, total_energy = run_highest_power_first(
                                    graph, deadline, green_power, interval_size, c, max_green_power, figure_file,
                                    task_ordering_criteria)
                            except Exception as e:
                                print(f'Error: {e}')

                            # Write data to csv file
                            data = {'experiment': i, 'workflow': graph_name, 'energy_trace': g_power_trace_name,
                                    'algorithm': 'highest_power_first', 'deadline': deadline,
                                    'task_ordering': task_ordering_criteria, 'deadline_factor': deadline_factor, 'c': c,
                                    'min_makespan': min_makespan, 'makespan': makespan,
                                    'brown_energy_used': brown_energy_used,
                                    'green_energy_not_used': green_energy_not_used, 'total_energy': total_energy}

                            row = []
                            for header in headers:
                                row.append(
                                    data[header]
                                )
                            csvwriter.writerow(row)

                            print()
                            i += 1

    report(f'Overall execution: %.4fs' % stopwatch.get_elapsed_time())
