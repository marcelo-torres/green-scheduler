import itertools
import math
import random
import unittest

from src.scheduling.algorithms.highest_power_first.highest_power_first import highest_power_first
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine_factory import create_machines_with_target
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_estimator import estimate_min_makespan
from src.scheduling.util.scheduling_check import check
from tests.scheduling.algorithms.highest_power_first.highest_power_first_sanity_test_data import load_workflows

MIN_POWER = 1
MAX_POWER = 5

class HighestPowerFirstSanityTest(unittest.TestCase):

    def test_workflows(self):
        random.seed(15735667867885)
        uniform = lambda: random.uniform(MIN_POWER, MAX_POWER)

        for workflow_provider in load_workflows():
            with self.subTest(msg=f'{workflow_provider[0]}'):

                graph = workflow_provider[1](uniform)
                clusters, deadline = self._create_clusters(graph, 1)
                schedule = highest_power_first(graph, deadline, 0.8, clusters, task_sort='energy', shift_mode='right-left')
                check(schedule, graph)

    def test_parameters(self):

        random.seed(15735667867885)
        uniform = lambda: random.uniform(MIN_POWER, MAX_POWER)
        workflow_providers = load_workflows()

        workflow_providers = filter(lambda w: w[0] in ['soykb', 'seismology'], workflow_providers)

        shift_modes = ['none', 'left', 'right-left']
        deadline_factors = [1, 4]
        c_values = [0, 0.5, 0.8]
        task_ordering_criterias = ['energy', 'power', 'runtime', 'runtime_ascending']

        for shift, d, c, ordering, workflow_provider in itertools.product(shift_modes, deadline_factors, c_values, task_ordering_criterias, workflow_providers):
            with self.subTest(msg=f'{workflow_provider[0]}_{shift}_{d}_{c}_{ordering}'):

                graph = workflow_provider[1](uniform)
                clusters, deadline = self._create_clusters(graph, d)
                schedule = highest_power_first(graph, deadline, c, clusters, task_sort=ordering, shift_mode=shift)
                check(schedule, graph)

    def _create_clusters(self, graph, deadline_factor):
        critical_path_length = calc_critical_path_length(graph)
        machines = create_machines_with_target(graph, critical_path_length * deadline_factor, [124], 0.40)

        min_makespan = estimate_min_makespan(graph, machines)
        deadline = min_makespan * deadline_factor

        machines = create_machines_with_target(graph, deadline, [124], 0.40)

        intervals = math.ceil(critical_path_length / 10)

        green_power_list = [random.uniform(MIN_POWER-1, MAX_POWER*5) for i in range(intervals)]

        power_series = PowerSeries('g1', green_power_list, 10)

        clusters = [Cluster('c1', power_series, machines)]

        return clusters, deadline
