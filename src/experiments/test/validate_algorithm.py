import random

from src.data.photovolta import PhotovoltaReader
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.drawer.active_tasks_drawer import ActiveTasksDrawer
from src.scheduling.drawer.ranks_drawer import draw
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.task_graph.task_graph import TaskGraph
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan


def _generate_random_power(min, max):
    return random.uniform(min, max)
def build_graph(tasks_per_rank):

    power_and_runtime_per_rank = [
        (10000, 10000),
        (8000, 8000),
        (6000, 7000),
        (3000, 5000),
        (2000, 4000),
        (1000, 2000)
    ]

    graph = TaskGraph()

    start_task_id = 0
    graph.add_new_task(start_task_id, runtime=0, power=0)  # Dummy task
    graph.set_start_task(start_task_id)

    tasks_of_previous_rank = [graph.get_task(start_task_id)]
    id = 1
    for total_power, total_runtime in power_and_runtime_per_rank:

        power = round(total_power / tasks_per_rank)
        runtime = total_runtime #round(total_runtime / tasks_per_rank)

        tasks_of_current_rank = []

        for i in range(tasks_per_rank):
            graph.add_new_task(id, runtime=runtime, power=power)
            tasks_of_current_rank.append(
                graph.get_task(id)
            )
            id += 1

        for parent in tasks_of_previous_rank:
            for child in tasks_of_current_rank:
                graph.create_dependency(parent.id, child.id)

        tasks_of_previous_rank = tasks_of_current_rank

    return graph

if __name__ == '__main__':
    resources_path = '../../../resources'

    # Green power data
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    interval_size = 300

    graph = build_graph(10)

    min_makespan = calc_critical_path_length(graph)
    scheduling = schedule_graph(graph, min_makespan * 32, green_power, interval_size, c=0.8, show='last',
                                max_power=15000, shift_mode='left')

    draw(graph, scheduling)

    calculator = EnergyUsageCalculator(green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(
        scheduling, graph)

    makespan = calc_makespan(scheduling, graph)
    max_active_tasks, mean, std, active_tasks_by_time = count_active_tasks(scheduling, graph)

    min_makespan = calc_critical_path_length(graph)

    print(f'\tMin makespan: {min_makespan}s')
    print(f'\tNumber of tasks: {len(graph.tasks)}')
    print(f'\tbrown_energy_used: {brown_energy_used}J')
    print(f'\tmakespan: {makespan}s')
    print(f'\tmax_active_tasks: {max_active_tasks}')
    print(f'\tactive_tasks_mean: {mean}')
    print(f'\tactive_tasks_std: {std}')

    ActiveTasksDrawer().draw(active_tasks_by_time)
