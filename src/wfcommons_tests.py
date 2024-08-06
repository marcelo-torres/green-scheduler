import os
import pathlib
from wfcommons.wfchef.recipes import EpigenomicsRecipe
from wfcommons import WorkflowGenerator, SrasearchRecipe

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons import _create_graph, _create_graph_from_real_trace
from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_calculator import calc_makespan


def report_graph(graph):
    min_makespan = calc_critical_path_length(graph)

    print(f'\tMin makespan: {min_makespan}s')
    print(f'\tNumber of tasks: {len(graph.tasks)}')

def execute_generator():
    resources_path = './../resources'

    min_task_power = 20
    max_task_power = 100
    max_green_power = 1000

    # Workflow generation
    synthetic_path = f'{resources_path}/wfcommons/synthetic'
    num_tasks = 10000
    runtime_factor = 2.0
    generator = WorkflowGenerator(SrasearchRecipe.from_num_tasks(num_tasks, runtime_factor=runtime_factor))
    workflow_name = f'{synthetic_path}/srasearch-workflow-t{num_tasks}-r{runtime_factor * 10}.json'
    if not os.path.isfile(workflow_name):
        for i, workflow in enumerate(generator.build_workflows(1)):
            workflow.write_json(pathlib.Path(workflow_name))

    # Workflow WfCommons to graph
    graph = _create_graph(workflow_name, min_task_power, max_task_power)
    report_graph(graph)

    # Green power data
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1()
    interval_size = 300

    min_makespan = calc_critical_path_length(graph)
    scheduling = schedule_graph(graph, min_makespan * 2, green_power, interval_size, c=0.3, show='last', max_power=max_green_power)

    calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling)

    makespan = calc_makespan(scheduling, graph)

    print(f'\tbrown_energy_used: {brown_energy_used}J')
    print(f'\tmakespan: {makespan}s')

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
    resources_path = './../resources'
    real_traces = f'{resources_path}/wfcommons/real_traces'

    genome = f'{real_traces}/1000genome-chameleon-8ch-250k-001.json'
    cycles = f'{real_traces}/cycles-chameleon-5l-3c-9p-001.json'
    montage = f'{real_traces}/montage-chameleon-dss-125d-001.json'
    seismology = f'{real_traces}/seismology-chameleon-900p-001.json'
    srasearch = f'{real_traces}/srasearch-chameleon-50a-005.json'

    real_traces_list = [genome, cycles, montage, seismology, srasearch]

    for trace_file in real_traces_list:
        print(trace_file)

        graph = _create_graph_from_real_trace(trace_file, 1, 100)
        min_makespan = calc_critical_path_length(graph)

        report_graph(graph)
        print()

if __name__ == '__main__':
    #execute_real_traces()
    execute_generator()
