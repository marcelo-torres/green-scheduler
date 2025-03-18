import unittest

from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.model.task_graph import TaskGraph

'''
     1---+
    / \  |
    2 3  |
    \ | /
      4
     / \
     5 6
     \ /
      7
'''


def _get_graph_1():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=10, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=7, power=10)
    graph.add_new_task(3, runtime=2, power=10)
    graph.add_new_task(4, runtime=4, power=10)
    graph.add_new_task(5, runtime=8, power=10)
    graph.add_new_task(6, runtime=9, power=10)
    graph.add_new_task(7, runtime=1, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    graph.create_dependency(2, 4)
    graph.create_dependency(3, 4)

    graph.create_dependency(4, 5)
    graph.create_dependency(4, 6)

    graph.create_dependency(5, 7)
    graph.create_dependency(6, 7)

    return graph, 31


'''
    1 -> 2 -> 3 -> 4 -> 5

'''
def _get_graph_2():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=3, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=70, power=10)
    graph.add_new_task(3, runtime=5, power=20)
    graph.add_new_task(4, runtime=1, power=10)
    graph.add_new_task(5, runtime=8, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(2, 3)
    graph.create_dependency(3, 4)
    graph.create_dependency(4, 5)

    return graph, 87


'''
        +-> 2
       /   
    1-+---> 3
       \     
        +-> 4
'''
def _get_graph_3():
    graph = TaskGraph()
    task = graph.add_new_task(1, runtime=5, power=10)
    graph.set_start_task(task.id)

    graph.add_new_task(2, runtime=10, power=10)
    graph.add_new_task(3, runtime=60, power=10)
    graph.add_new_task(4, runtime=50, power=10)

    graph.create_dependency(1, 2)
    graph.create_dependency(1, 3)
    graph.create_dependency(1, 4)

    return graph, 65


def create_single_machine_cluster(green_power, interval_length, cores=100):
    power_series = PowerSeries('g1', green_power, interval_length)
    machine = Machine('m1', cores, 0)
    return [
        Cluster('c1', power_series, [machine])
    ]

def create_two_machine_cluster(green_power, interval_length, cores=100):
    power_series = PowerSeries('g1', green_power, interval_length)
    machine1 = Machine('m1', cores, 0)
    machine2 = Machine('m2', cores, 0)
    return [
        Cluster('c1', power_series, [machine1, machine2])
    ]

class HighestPowerFirstTest(unittest.TestCase):

    def test_single_task_with_no_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([], 0)

        scheduling = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[task.id][0])

    def test_single_task_with_no_g_energy_and_no_slack(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([], 0)

        scheduling = highest_power_first(graph, task.runtime * 2, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[task.id][0])

    def test_single_task_with_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([0, 10, 0, 10], 40)

        scheduling = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(40, scheduling[task.id][0])

    def test_single_task_with_increasing_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([0, 10, 20, 30, 40, 50, 60], 20)

        scheduling = highest_power_first(graph, task.runtime * 2, 0.5, clusters, task_sort='energy',
                                         shift_mode='left')

        self.assertEqual(20, scheduling[task.id][0])

    def test_single_task_with_decreasing_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([60, 50, 40, 30, 20, 10, 0], 20)

        scheduling = highest_power_first(graph, task.runtime * 2, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[task.id][0])

    def test_single_task_with_variable_g_energy_1(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([0, 10, 20, 10, 0, 0, 0, 5, 10, 5, 0, 0, 0, 50, 20, 10, 0], 20)

        scheduling = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[task.id][0])

    def test_single_task_with_variable_g_energy_2(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=10)
        graph.set_start_task(task.id)

        clusters = create_single_machine_cluster([0, 10, 20, 10, 0, 0, 0, 5, 10, 5, 0, 0, 0, 50, 20, 10, 1], 20)

        scheduling = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(240, scheduling[task.id][0])

    def test_multiple_tasks_g1_with_no_g_energy(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_single_machine_cluster([], 0)

        scheduling = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[1][0])
        self.assertEqual(10, scheduling[2][0])
        self.assertEqual(10, scheduling[3][0])
        self.assertEqual(17, scheduling[4][0])
        self.assertEqual(21, scheduling[5][0])
        self.assertEqual(21, scheduling[6][0])
        self.assertEqual(30, scheduling[7][0])

    def test_multiple_tasks_g2_with_no_g_energy(self):
        graph, min_makespan = _get_graph_2()

        clusters = create_single_machine_cluster([], 0)

        scheduling = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(0, scheduling[1][0])
        self.assertEqual(3, scheduling[2][0])
        self.assertEqual(73, scheduling[3][0])
        self.assertEqual(78, scheduling[4][0])
        self.assertEqual(79, scheduling[5][0])

    def test_multiple_tasks_g1_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_single_machine_cluster([0, 10, 20, 20, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        scheduling = highest_power_first(graph, min_makespan * 2, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(10, scheduling[1][0])
        self.assertEqual(20, scheduling[2][0])
        self.assertEqual(20, scheduling[3][0])
        self.assertEqual(27, scheduling[4][0])
        self.assertEqual(31, scheduling[5][0])
        self.assertEqual(31, scheduling[6][0])
        self.assertEqual(50, scheduling[7][0])

    def test_multiple_tasks_g1_with_no_slack_to_delay_and_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        clusters = create_single_machine_cluster([0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        scheduling = highest_power_first(graph, min_makespan * 2, 0.5, clusters, task_sort='energy', shift_mode='left', show='none')

        self.assertEqual(10, scheduling[1][0])
        self.assertEqual(20, scheduling[2][0])
        self.assertEqual(20, scheduling[3][0])
        self.assertEqual(27, scheduling[4][0])

        self.assertEqual(47, scheduling[6][0])
        self.assertEqual(31, scheduling[5][0])
        self.assertEqual(57, scheduling[7][0])

    def test_multiple_tasks_g1_with_no_slack_to_delay_and_with_g_energy_and_right_left(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        clusters = create_single_machine_cluster([0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        scheduling = highest_power_first(graph, min_makespan * 2, 0.5, clusters, task_sort='energy', shift_mode='right-left')

        self.assertEqual(10, scheduling[1][0])
        self.assertEqual(20, scheduling[2][0])
        self.assertEqual(20, scheduling[3][0])
        self.assertEqual(27, scheduling[4][0])
        self.assertEqual(31, scheduling[5][0])
        self.assertEqual(50, scheduling[6][0])
        self.assertEqual(60, scheduling[7][0])

    def test_multiple_tasks_g1_with_slack_to_delay_and_with_g_energy(self):
        graph, min_makespan = _get_graph_1()

        min_makespan += 1
        graph.get_task(6).runtime = 10

        clusters = create_single_machine_cluster([0, 10, 20, 10, 0, 20, 40, 20, 0, 0, 0, 30, 0], 10)

        scheduling = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertEqual(10, scheduling[1][0])
        self.assertEqual(20, scheduling[2][0])
        self.assertEqual(20, scheduling[3][0])
        self.assertEqual(27, scheduling[4][0])
        self.assertEqual(31, scheduling[5][0])
        self.assertEqual(50, scheduling[6][0])
        self.assertEqual(60, scheduling[7][0])

    def test_runtime_ascending(self):
        graph, min_makespan = _get_graph_3()

        clusters = create_single_machine_cluster([10, 10], 100)

        scheduling = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='runtime_ascending', shift_mode='left')

        scheduling = list(scheduling.items())
        scheduling.sort(key=lambda schedule: schedule[1][0])

        tasks = list(
            map(lambda schedule: schedule[0], scheduling)
        )

        self.assertEqual(1, tasks[0])
        self.assertEqual(2, tasks[1])
        self.assertEqual(4, tasks[2])
        self.assertEqual(3, tasks[3])


    def test_multi_machine_single_task_with_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=25)
        graph.set_start_task(task.id)

        clusters = create_two_machine_cluster([10, 50, 20], 50, cores=1)

        schedule = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertTaskIn(schedule, 1, 50, 'm1')

    def test_multi_machine_single_task_with_no_g_energy(self):
        graph = TaskGraph()
        task = graph.add_new_task(1, runtime=100, power=25)
        graph.set_start_task(task.id)

        clusters = create_two_machine_cluster([0, 0, 0], 50, cores=1)

        schedule = highest_power_first(graph, task.runtime * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')

    def test_multi_machine_pipeline_cores_with_g_energy(self):
        graph, min_makespan = _get_graph_2()

        clusters = create_two_machine_cluster([10, 50, 0, 20], 25, cores=1)

        schedule = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 3, 'm1')
        self.assertTaskIn(schedule, 3, 75, 'm1')
        self.assertTaskIn(schedule, 4, 80, 'm1')
        self.assertTaskIn(schedule, 5, 81, 'm1')

    def test_multi_machine_pipeline_with_cores_with_no_g_energy(self):
        graph, min_makespan = _get_graph_2()

        clusters = create_two_machine_cluster([0]*10, 25, cores=1)

        schedule = highest_power_first(graph, min_makespan * 4, 0.2, clusters, task_sort='energy', shift_mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 3, 'm1')
        self.assertTaskIn(schedule, 3, 73, 'm1')
        self.assertTaskIn(schedule, 4, 78, 'm1')
        self.assertTaskIn(schedule, 5, 79, 'm1')

    def test_multi_machine_pipeline_with_no_cores_with_g_energy(self):
        graph, min_makespan = _get_graph_2()

        clusters = create_two_machine_cluster([10, 50, 0, 20], 25, cores=1)
        clusters[0].machines['m1'].state.use_cores(0, 200, 1)

        schedule = highest_power_first(graph, min_makespan * 4, 0.5, clusters, task_sort='energy', shift_mode='left')

        self.assertTaskIn(schedule, 1, 0, 'm2')
        self.assertTaskIn(schedule, 2, 3, 'm2')
        self.assertTaskIn(schedule, 3, 75, 'm2')
        self.assertTaskIn(schedule, 4, 80, 'm2')
        self.assertTaskIn(schedule, 5, 81, 'm2')


    def test_multi_machine_graph_with_cores_with_g_energy_shift_left(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 10, 20, 20, 20, 20, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10], 5,
                                              cores=1)

        schedule = highest_power_first(graph, 90, 0.2, clusters, task_sort='energy', shift_mode='left', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm1')

        self.assertTaskIn(schedule, 2, 10, 'm1')
        self.assertTaskIn(schedule, 3, 10, 'm2')
        self.assertTaskIn(schedule, 4, 17, 'm1')
        self.assertTaskIn(schedule, 5, 21, 'm1')
        self.assertTaskIn(schedule, 6, 21, 'm2')
        self.assertTaskIn(schedule, 7, 30, 'm1')

    def test_multi_machine_graph_with_no_cores_with_g_energy_shift_left(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 10, 20, 20, 20, 20, 10, 10, 10, 0, 0, 0, 0, 0, 0, 0, 10], 5,
                                              cores=1)

        clusters[0].machines['m1'].state.use_cores(0, 90, 1)
        schedule = highest_power_first(graph, 90, 0.2, clusters, task_sort='energy', shift_mode='left', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm2')
        self.assertTaskIn(schedule, 2, 10, 'm2')
        self.assertTaskIn(schedule, 3, 17, 'm2')
        self.assertTaskIn(schedule, 4, 19, 'm2')
        self.assertTaskIn(schedule, 5, 23, 'm2')
        self.assertTaskIn(schedule, 6, 31, 'm2')
        self.assertTaskIn(schedule, 7, 40, 'm2')

    # def test_multi_machine_graph_with_no_cores_with_no_g_energy_shift_left(self):
    #     graph, min_makespan = _get_graph_1()
    #
    #     clusters = create_two_machine_cluster([10, 10, 20, 20, 20, 20, 10, 10, 10, 0, 0, 0, 0, 0, 0, 0, 10], 5,
    #                                           cores=1)
    #
    #     clusters[0].machines['m1'].state.use_cores(0, 90, 1)
    #     clusters[0].machines['m2'].state.use_cores(0, 60, 1)
    #     schedule = highest_power_first(graph, 90, 0.2, clusters, task_sort='energy', shift_mode='left', show='none')

    def test_multi_machine_graph_with_cores_with_g_energy_shift_rigth_left(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 10, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)

        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='right-left', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm1')

        self.assertTaskIn(schedule, 2, 10, 'm1')
        self.assertTaskIn(schedule, 3, 20, 'm1')
        self.assertTaskIn(schedule, 4, 22, 'm1')
        self.assertTaskIn(schedule, 5, 55, 'm1')
        self.assertTaskIn(schedule, 6, 26, 'm1')
        self.assertTaskIn(schedule, 7, 63, 'm1')

    def test_multi_machine_graph_with_cores_with_g_energy_shift_rigth_left_2(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 20, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)

        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='right-left', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm1')

        self.assertTaskIn(schedule, 2, 10, 'm1')
        self.assertTaskIn(schedule, 3, 10, 'm2')
        self.assertTaskIn(schedule, 4, 20, 'm1')
        self.assertTaskIn(schedule, 5, 55, 'm1')
        self.assertTaskIn(schedule, 6, 24, 'm1')
        self.assertTaskIn(schedule, 7, 63, 'm1')

    def test_multi_machine_graph_with_no_cores_with_g_energy_shift_rigth_left(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 20, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)

        clusters[0].machines['m1'].state.use_cores(0, 23, 1)
        clusters[0].machines['m2'].state.use_cores(23, 90, 1)
        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='right-left', show='none')


        #clusters[0].machines['m2'].state.use_cores(0, 60, 1)

        self.assertTaskIn(schedule, 1, 0, 'm2')
        self.assertTaskIn(schedule, 2, 10, 'm2')
        self.assertTaskIn(schedule, 3, 17, 'm2')
        self.assertTaskIn(schedule, 4, 19, 'm2')
        self.assertTaskIn(schedule, 5, 55, 'm1')
        self.assertTaskIn(schedule, 6, 23, 'm1')
        self.assertTaskIn(schedule, 7, 63, 'm1')

    def test_multi_machine_graph_with_no_cores_with_g_energy_shift_rigth_left_2(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 20, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)

        clusters[0].machines['m1'].state.use_cores(0, 22, 1)
        clusters[0].machines['m2'].state.use_cores(23, 90, 1)
        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='right-left', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm2')
        self.assertTaskIn(schedule, 2, 10, 'm2')
        self.assertTaskIn(schedule, 3, 20, 'm2')
        self.assertTaskIn(schedule, 4, 22, 'm1')
        self.assertTaskIn(schedule, 5, 55, 'm1')
        self.assertTaskIn(schedule, 6, 26, 'm1')
        self.assertTaskIn(schedule, 7, 63, 'm1')

    def test_multi_machine_graph_with_cores_with_g_energy_shift_none(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 10, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)

        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='none', show='none')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 10, 'm1')
        self.assertTaskIn(schedule, 3, 10, 'm2')
        self.assertTaskIn(schedule, 4, 17, 'm1')
        self.assertTaskIn(schedule, 5, 25, 'm2')
        self.assertTaskIn(schedule, 6, 21, 'm1')
        self.assertTaskIn(schedule, 7, 33, 'm1')

    def test_multi_machine_graph_with_no_cores_with_g_energy_shift_none(self):
        graph, min_makespan = _get_graph_1()

        clusters = create_two_machine_cluster([10, 20, 10, 0, 15, 20, 10, 0, 15, 5, 0, 10, 10, 10, 0, 0, 0, 15], 5,
                                              cores=1)
        clusters[0].machines['m2'].state.use_cores(0, 200, 1)

        schedule = highest_power_first(graph, 90, 0, clusters, task_sort='energy', shift_mode='none')

        self.assertTaskIn(schedule, 1, 0, 'm1')
        self.assertTaskIn(schedule, 2, 10, 'm1')
        self.assertTaskIn(schedule, 3, 17, 'm1')
        self.assertTaskIn(schedule, 4, 19, 'm1')
        self.assertTaskIn(schedule, 5, 55, 'm1')
        self.assertTaskIn(schedule, 6, 23, 'm1')
        self.assertTaskIn(schedule, 7, 63, 'm1')

    def assertTaskIn(self, schedule, task_id, expected_start_time, expected_machine_id):
        self.assertEqual(expected_start_time, schedule[task_id][0])
        self.assertEqual(expected_machine_id, schedule[task_id][1])