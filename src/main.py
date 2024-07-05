from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.task_graph.task_graph import TaskGraph


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
            scheduling = schedule_graph(graph, 124, green_power, interval_size, c=c, show='last')
            calculator = EnergyUsageCalculator(graph, green_power, interval_size)
            brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling)
            last_task = graph.get_task(7)
            makespan = scheduling[last_task.id] + last_task.runtime
            print(f'c={c}:\tbrown_energy_used: {brown_energy_used}J | makespan: {makespan}s')
        print()


def run_single_test():
    graph = create_graph()
    draw_task_graph(graph)

    interval_size = 13
    green_power = [20, 40, 30, 20, 10, 5, 3, 2, 1, 4, 5, 6, 8, 5]

    scheduling = schedule_graph(graph, 124, green_power, interval_size, c=0.50, show='all')

    calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling)
    last_task = graph.get_task(7)
    makespan = scheduling[last_task.id] + last_task.runtime

    print(f'brown_energy_used: {brown_energy_used}J | makespan: {makespan}s')

if __name__ == '__main__':
    #run_all_tests()
    run_single_test()

