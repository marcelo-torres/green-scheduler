import os
import pathlib
from wfcommons.wfchef.recipes import EpigenomicsRecipe
from wfcommons import WorkflowGenerator, SrasearchRecipe, MontageRecipe, SeismologyRecipe, SoykbRecipe

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import _create_graph, _create_graph_from_real_trace
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.drawer.active_tasks_drawer import ActiveTasksDrawer
from src.scheduling.drawer.ranks_drawer import draw
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.count_active_tasks import count_active_tasks
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.topological_ordering import calculate_upward_rank

import random
MIN_TASK_POWER_DEFAULT = 1
MAX_TASK_POWER_DEFAULT = 5


def random_gauss():
    mu = 3  # Mean
    sigma = 0.9  # Standard deviation

    value = random.gauss(mu, sigma)
    while value < MIN_TASK_POWER_DEFAULT or value > MAX_TASK_POWER_DEFAULT:
        value = random.gauss(mu, sigma)
    return value


def report_graph(graph):
    min_makespan = calc_critical_path_length(graph)

    print(f'\tMin makespan: {min_makespan}s')
    print(f'\tNumber of tasks: {len(graph.tasks)}')


def reduce_workflow_srasearch(graph):
    ranks = calculate_upward_rank(graph)

    rank_task_count = {}

    end_task_id = None

    for task_id, rank in ranks.items():
        if rank not in rank_task_count:
            rank_task_count[rank] = 1
        else:
            rank_task_count[rank] += 1

        if rank == 4:
            end_task_id = task_id

    for rank, count in rank_task_count.items():
        print(f'rank {rank}: {count} tasks')

    for task_id, rank in ranks.items():
        if rank == 2 or rank == 3:
            graph.remove_task(task_id)
        if rank == 1:
            graph.create_dependency(task_id, end_task_id)

def execute_generator():
    resources_path = '../../../resources'

    min_task_power = 1
    max_task_power = 5
    max_green_power = 1000

    # Workflow generation
    synthetic_path = f'{resources_path}/wfcommons/synthetic'
    num_tasks = 24 #98 #24 #10000
    runtime_factor = 2.0
    generator = WorkflowGenerator(SrasearchRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor))
    workflow_name = f'{synthetic_path}/srasearch-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    # generator = WorkflowGenerator(SoykbRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor))
    # workflow_name = f'{synthetic_path}/soykb-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    # generator = WorkflowGenerator(MontageRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor))
    # workflow_name = f'{synthetic_path}/montage-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    # generator = WorkflowGenerator(SeismologyRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor))
    # workflow_name = f'{synthetic_path}/seismology-workflow-t{num_tasks}-r{runtime_factor * 10}.json'

    if not os.path.isfile(workflow_name):
        for i, workflow in enumerate(generator.build_workflows(1)):
            workflow.write_json(pathlib.Path(workflow_name))

    # Workflow WfCommons to graph
    graph = _create_graph(workflow_name, random_gauss)

    #reduce_workflow_srasearch(graph)
    report_graph(graph)

    #draw_task_graph(graph)

    # Green power data
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)
    #green_power = [5, 10, 20, 40, 10, 5] * 100
    interval_size = 300

    min_makespan = calc_critical_path_length(graph)
    scheduling = schedule_graph(graph, min_makespan * 4, green_power, interval_size, c=0.8, show='all', max_power=max_green_power, shift_mode='none', task_ordering='runtime_ascending')

    #draw(graph, scheduling)

    calculator = EnergyUsageCalculator(green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling, graph)

    makespan = calc_makespan(scheduling, graph)
    max_active_tasks, mean, std, active_tasks_by_time = count_active_tasks(scheduling, graph)

    print(f'\tbrown_energy_used: {brown_energy_used:.{2}f}J')
    print(f'\tmakespan: {makespan}s')
    print(f'\tmax_active_tasks: {max_active_tasks}')
    print(f'\tactive_tasks_mean: {mean:.{2}f}')
    print(f'\tactive_tasks_std: {std:.{2}f}')

    #ActiveTasksDrawer().draw(active_tasks_by_time)



    print()

# generator = WorkflowGenerator(EpigenomicsRecipe.from_num_tasks(10000, runtime_factor=2.1))
# for i, workflow in enumerate(generator.build_workflows(1)):
#
#
# graph = _create_graph('epigenomics-workflow-0.json', 1, 100)
#graph = _create_graph_from_real_trace('montage-chameleon-dss-20d-001.json', 1, 100)
#min_makespan = calc_critical_path_length(graph)
#print(min_makespan)

def execute_real_traces():
    resources_path = '../../../resources'
    real_traces = f'{resources_path}/wfcommons/real_traces'

    genome = f'{real_traces}/1000genome-chameleon-8ch-250k-001.json'
    cycles = f'{real_traces}/cycles-chameleon-5l-3c-9p-001.json'
    montage = f'{real_traces}/montage-chameleon-dss-125d-001.json'
    seismology = f'{real_traces}/seismology-chameleon-900p-001.json'
    srasearch = f'{real_traces}/srasearch-chameleon-50a-005.json'

    real_traces_list = [genome, cycles, montage, seismology, srasearch]

    for trace_file in real_traces_list:
        print(trace_file)


if __name__ == '__main__':
    #execute_real_traces()
    execute_generator()
