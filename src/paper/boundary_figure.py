from src.scheduling.algorithms.bounded_boundary_search.bounded_boundary_search import bbs
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.algorithms.task_flow.task_flow import task_flow_schedule
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import create_single_machine_cluster
from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length


def create_task_flow_graph():
    graph = TaskGraph()
    graph.set_start_task(0)

    graph.add_new_task(0, runtime=0, power=10)
    graph.add_new_task(1, runtime=10, power=10)
    graph.add_new_task(2, runtime=15, power=15)
    graph.add_new_task(3, runtime=20, power=10)
    graph.add_new_task(4, runtime=7, power=30)
    graph.add_new_task(5, runtime=14, power=20)
    graph.add_new_task(6, runtime=12, power=20)
    graph.add_new_task(7, runtime=8, power=20)

    graph.create_dependency(0, 1)
    graph.create_dependency(0, 2)

    graph.create_dependency(1, 3)
    graph.create_dependency(2, 3)

    graph.create_dependency(3, 4)
    graph.create_dependency(3, 5)
    graph.create_dependency(3, 6)

    graph.create_dependency(4, 6)

    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph

def create_graph_old():
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

def create_graph():
    graph = TaskGraph()
    graph.set_start_task(1)

    graph.add_new_task(1, runtime=5, power=10)
    graph.add_new_task(2, runtime=12, power=15)
    graph.add_new_task(3, runtime=15, power=10)
    graph.add_new_task(4, runtime=20, power=30)
    graph.add_new_task(5, runtime=20, power=20)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(2, 4)
    graph.create_dependency(3, 4)
    graph.create_dependency(4, 5)

    return graph

def report_schedule(schedule, graph, last_task_id, clusters):

    min_makespan = calc_critical_path_length(graph)
    print(f'min makespan: {min_makespan}s')

    calculator = EnergyUsageCalculator(clusters.power_series.green_power_list, clusters.power_series.interval_length)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(schedule, graph)
    last_task = graph.get_task(5)
    makespan = schedule[last_task.id][0] + last_task.runtime

    print(f'brown_energy_used: {brown_energy_used}J | total_energy: {total_energy} | makespan: {makespan}s | min_makespan: {min_makespan}')


def plot_bounded_boundary_search_figure(graph, cluster):
    min_makespan = calc_critical_path_length(graph)
    scheduling = bbs(graph, min_makespan * 3, 0.5, [cluster], task_sort='energy', shift_mode='left', show='all', max_power=35)

    report_schedule(scheduling, graph, 5, cluster)


def plot_task_flow_figure(graph, cluster):

    schedule = task_flow_schedule(graph, [cluster], show='all', max_power=35, chart_x_end=180, graph_boundaries=False)
    report_schedule(schedule, graph, 5, cluster)

def plot_lpt_figure(graph, cluster):

    schedule = lpt(graph, [cluster], show='last', max_power=35, chart_x_end=180)
    report_schedule(schedule, graph, 5, cluster)


if __name__ == '__main__':
    graph = create_graph()
    cluster = create_single_machine_cluster([5, 15, 35, 30, 25, 15, 8, 0, 5, 15, 15, 30, 40], 22)

    plot_bounded_boundary_search_figure(graph, cluster)
    #plot_task_flow_figure(graph, cluster)
    #plot_lpt_figure(graph, cluster)


