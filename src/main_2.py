import time

from src.data.photovolta import PhotovoltaReader
from src.data.workflow_parquet_reader import WorkflowTraceArchiveReader
from src.scheduling.algorithms.highest_power_first.calc_levels import calc_levels
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.energy_usage_calculator import EnergyUsageCalculator
from src.task_graph.task_graph import TaskGraph


# Project structure https://docs.python-guide.org/writing/structure/
# Naming conventions https://visualgit.readthedocs.io/en/latest/pages/naming_convention.html

def create_graph():
    graph = TaskGraph()
    graph.set_start_task(0)
    graph.add_new_task(0, runtime=0, power=0) # Dummy task
    graph.add_new_task(1, runtime=10, power=14)
    graph.add_new_task(2, runtime=15, power=10)
    graph.add_new_task(3, runtime=20, power=12)
    graph.add_new_task(4, runtime=7, power=18)
    graph.add_new_task(5, runtime=14, power=14)
    graph.add_new_task(6, runtime=12, power=16)
    graph.add_new_task(7, runtime=8, power=4)

    graph.create_dependency(0, 1)
    graph.create_dependency(0, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(2, 3)
    graph.create_dependency(3, 6)
    graph.create_dependency(3, 4)
    graph.create_dependency(3, 5)
    graph.create_dependency(4, 6)
    graph.create_dependency(4, 7)
    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph


def run_all_tests():
    green_intervals = [
        ('#1', 10, [2, 7, 10, 18, 23, 27, 30, 27, 24, 21, 18, 14, 7]),
        ('#2', 10, [20, 40, 30, 20, 10, 5, 3, 2, 1, 4, 5, 6, 8, 5]),
        ('#3', 10, [40, 20, 30, 20, 10, 5, 1, 5, 10, 20, 30, 20, 40, 8])
    ]

    c_values = [0, 0.75]
    #c_values = [0, 0.1, 0.25, 0.5, 0.75]

    graph = create_graph()

    for name, interval_size, green_power in green_intervals:
        for c in c_values:
            print(c)
            scheduling = schedule_graph(graph, 124, green_power, interval_size, c=c, show='all')
            calculator = EnergyUsageCalculator(graph, green_power, interval_size)
            brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage(scheduling)
            last_task = graph.get_task(7)
            makespan = scheduling[last_task.id] + last_task.runtime
            print(f'c={c}:\tbrown_energy_used: {brown_energy_used}J | makespan: {makespan}s')
        print()

def calc_critical_path_length(graph):
    levels, max_level =  calc_levels(graph)

    max_of_level = {}

    for task_id, level in levels.items():
        task = graph.get_task(task_id)

        if level not in max_of_level:
            max_of_level[level] = task.runtime
        else:
            current_max = max_of_level[level]
            if task.runtime > current_max:
                max_of_level[level] = task.runtime

    critical_path_length = 0
    for level, max_runtime in max_of_level.items():
        critical_path_length += max_runtime

    return critical_path_length

def get_makespan(scheduling, graph):

    max_finish_time = -1

    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)
        finish_time = start_time + task.runtime

        if finish_time > max_finish_time:
            max_finish_time = finish_time

    return max_finish_time


def run_single_test():
    resources_path = './../resources'

    min_task_power = 20
    max_task_power = 100
    max_green_power = 1000

    reader = WorkflowTraceArchiveReader(resources_path, min_task_power, max_task_power)
    photovoltaReader = PhotovoltaReader(resources_path)

    graph = reader.epigenomics()

    min_makespan = calc_critical_path_length(graph)

    print(f'min_makespan: {min_makespan}s')

    #green_power = [20, 40, 30, 20, 10, 5, 3, 2, 1, 4, 5, 6, 8, 5]
    #green_power = [gp * 1 for gp in green_power]

    green_power = photovoltaReader.get_trace_1()
    interval_size = 10 #int(min_makespan / len(green_power)) + 1

    scheduling = schedule_graph(graph, min_makespan * 2, green_power, interval_size, c=0.30, show='off', max_power=max_green_power)

    calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage(scheduling)

    makespan = get_makespan(scheduling, graph)

    print(f'brown_energy_used: {brown_energy_used}J | makespan: {makespan}s')

if __name__ == '__main__':
    start_time = time.time()

    #run_all_tests()
    run_single_test()

    print("--- %s seconds ---" % (time.time() - start_time))

