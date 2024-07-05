import csv
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


def get_file_name(dt):
    return dt.strftime("experiments_%Y-%m-%d_%H-%M-%S.csv")


def run_highest_power_first(graph, deadline, green_power, interval_size, c, max_green_power):
    scheduling = schedule_graph(graph, deadline, green_power, interval_size, c=c, show='off', max_power=max_green_power)

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


if __name__ == '__main__':

    resources_path = './../resources'
    min_task_power = 20
    max_task_power = 100

    reader = WorkflowTraceArchiveReader(resources_path, min_task_power, max_task_power)
    photovoltaReader = PhotovoltaReader(resources_path)

    interval_size = 100
    max_green_power = 1000

    graph_providers = [
        ('epigenomics', reader.epigenomics),
        ('montage', reader.montage)
    ]
    green_power_providers = [
        ('trace_1', photovoltaReader.get_trace_1),
        ('trace_2', photovoltaReader.get_trace_2)
    ]

    deadline_factors = [1, 2, 4, 8, 10]
    c_values = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]

    experiments_count = len(graph_providers) * len(green_power_providers) * len(deadline_factors) * len(c_values)
    start_time = datetime.now()
    report(f'[{format_date(start_time)}] Starting {experiments_count} experiments...\n')

    experiments_reports_path = resources_path + '/experiments'
    file_full_path = experiments_reports_path + '/' + get_file_name(start_time)

    with open(file_full_path, 'x') as csvfile:

        stopwatch = Stopwatch()
        stopwatch.start()

        headers = ['experiment', 'workflow', 'energy_trace', 'algorithm', 'deadline', 'deadline_factor', 'c',
                   'min_makespan', 'makespan', 'brown_energy_used', 'green_energy_not_used', 'total_energy']
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)

        i = 1
        for graph_name, graph_provider in graph_providers:
            for g_power_trace_name, green_power_provider in green_power_providers:
                for deadline_factor in deadline_factors:
                    for c in c_values:
                        report(
                            f'{i}/{experiments_count}: {graph_name} {g_power_trace_name} deadline_factor={deadline_factor} c={c}')

                        graph = graph_provider()
                        green_power = green_power_provider()

                        min_makespan = calc_critical_path_length(graph)
                        deadline = deadline_factor * min_makespan
                        report(f'\tmin_makespan: {min_makespan}s')

                        try:
                            #draw_task_graph(graph)
                            makespan, brown_energy_used, green_energy_not_used, total_energy = run_highest_power_first(
                                graph, deadline, green_power, interval_size, c, max_green_power)
                        except Exception as e:
                            print(f'Error: {e}')

                        # Write data to csv file
                        data = {'experiment': i, 'workflow': graph_name, 'energy_trace': g_power_trace_name,
                                'algorithm': 'highest_power_first', 'deadline': deadline,
                                'deadline_factor': deadline_factor, 'c': c, 'min_makespan': min_makespan,
                                'makespan': makespan, 'brown_energy_used': brown_energy_used,
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
