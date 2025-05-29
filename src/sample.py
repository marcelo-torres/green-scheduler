import os
import random

from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsRealWorkflowReader
from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first, SHIFT_MODE_NONE, \
    TASK_SORT_ENERGY, BOUNDARY_SINGLE
from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.drawer.task_graph_drawer import draw_task_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.model.task_graph import TaskGraph
from src.scheduling.util.makespan_calculator import calc_makespan
from src.scheduling.util.scheduling_check import check


def _get_graph():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=10, power=35)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=14, power=9)
    graph.add_new_task(3, runtime=7, power=10)
    graph.add_new_task(4, runtime=17, power=34)
    graph.add_new_task(5, runtime=13, power=56)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    graph.create_dependency(2, 5)
    graph.create_dependency(3, 5)
    graph.create_dependency(4, 5)

    return graph, 34


def check_integrity(schedule, graph):
    scheduling_violations = check(schedule, graph)
    if len(scheduling_violations) > 0:
        print(f'\n {len(scheduling_violations)} scheduling violations were found')
        for task_id, start, pred_id, pred_finish_time in scheduling_violations:
            print(f'Task {task_id} start: {start} | Pred {pred_id} finish time: {pred_finish_time}')


def report(schedule, graph, power_series):
    calculator = EnergyUsageCalculator(power_series.green_power_list, power_series.interval_length)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(schedule, graph)
    makespan = calc_makespan(schedule, graph)
    print(f'Makespan: {makespan}s\tBrown Energy used: {brown_energy_used}J\tGreen Energy used: {total_energy-brown_energy_used}J')


def run_example():

    print('-- Example --')

    green_power = [5, 4, 20, 40, 30, 50, 40, 45, 8, 5]
    power_series = PowerSeries('g1', green_power, 20)

    cluster = Cluster('c1', power_series, [
        Machine('m1', 1, 0),
        Machine('m2', 5, 0)
    ])

    graph, min_makespan = _get_graph()

    draw_task_graph(graph, with_labels=True)

    c = 0.3
    df = 4

    schedule = highest_power_first(graph, min_makespan * df, c, [cluster],
                                         task_sort=TASK_SORT_ENERGY,  # TASK_SORT_POWER, TASK_SORT_RUNTIME, TASK_SORT_RUNTIME_ASCENDING
                                         shift_mode=SHIFT_MODE_NONE,  # SHIFT_MODE_LEFT or SHIFT_MODE_RIGHT_LEFT,
                                         show='all'  #  'last' to show only last step
                                     )
    check_integrity(schedule, graph)
    report(schedule, graph, power_series)

def run_real_workflow():

    print('\n-- Real Workflow --')

    resources_path = './../resources'

    def _generate_random_power():
        return random.uniform(50, 100)

    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=1)
    interval_size = 300

    power_series = PowerSeries('g1', green_power, interval_size)
    cluster = Cluster('c1', power_series, [
        Machine('m1', 500, 0),
        Machine('m2', 500, 0)
    ])

    reader = WfCommonsRealWorkflowReader(resources_path + '/wfcommons/real_traces')
    workflow = reader.get_soykb(_generate_random_power)

    c = 0.3
    df = 4

    lpt_schedule = lpt(workflow, [cluster])
    min_makespan = calc_makespan(lpt_schedule, workflow)

    schedule = highest_power_first(workflow, min_makespan * df, c, [cluster],
                                   task_sort=TASK_SORT_ENERGY,
                                   # TASK_SORT_POWER, TASK_SORT_RUNTIME, TASK_SORT_RUNTIME_ASCENDING
                                   shift_mode=SHIFT_MODE_NONE,  # SHIFT_MODE_LEFT or SHIFT_MODE_RIGHT_LEFT,
                                   boundary_strategy=BOUNDARY_SINGLE,
                                   show='last'  # 'all' to show every step
                                   )

    check_integrity(schedule, workflow)
    report(schedule, workflow, power_series)

if __name__ == '__main__':
    run_example()
    run_real_workflow()
