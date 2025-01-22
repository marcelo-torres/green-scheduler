from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.task_graph.task_graph import TaskGraph
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length


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
    graph.add_new_task(2, runtime=14, power=15)
    graph.add_new_task(3, runtime=14, power=10)
    graph.add_new_task(4, runtime=20, power=30)
    graph.add_new_task(5, runtime=20, power=20)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(2, 4)
    graph.create_dependency(3, 4)
    graph.create_dependency(4, 5)

    return graph


def run_single_test():
    graph = create_graph()
    #draw_task_graph(graph, with_labels=True)

    #interval_size = 15
    #green_power = [5, 15, 35, 40, 25, 15, 8, 0, 5, 15, 15, 30, 40]

    interval_size = 22
    green_power = [5, 15, 35, 30, 25, 15, 8, 0, 5, 15, 15, 30, 40]

    min_makespan = calc_critical_path_length(graph)
    print(f'min makespan: {min_makespan}s')

    scheduling = schedule_graph(graph, min_makespan * 3, green_power, interval_size, c=0.0, show='last', task_ordering='energy', max_power=50, shift_mode='left')

    calculator = EnergyUsageCalculator(green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling, graph)
    last_task = graph.get_task(5)
    makespan = scheduling[last_task.id] + last_task.runtime

    print(f'brown_energy_used: {brown_energy_used}J | total_energy: {total_energy} | makespan: {makespan}s | min_makespan: {min_makespan}')

if __name__ == '__main__':
    #run_all_tests()
    run_single_test()

