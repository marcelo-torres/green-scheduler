from src.data.photovolta import PhotovoltaReader
from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.experiments.main.makespan_estimator_impl import estimate_min_makespan_by_algorithm, OPTION_LCB_LPT, OPTION_LCB, \
    OPTION_PATH_LPT
from src.experiments.main.run_experiments import SEED, MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT
from src.experiments.random_utils import RandomProvider
from src.scheduling.algorithms.bounded_boundary_search.bounded_boundary_search import bbs, BOUNDARY_LPT, \
    BOUNDARY_DEFAULT, BOUNDARY_LPT_PATH, BOUNDARY_LPT_FULL
from src.scheduling.model.cluster import Cluster
from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries
from src.scheduling.util.makespan_calculator import calc_makespan


def calc_min_makespans_of_workflows(resources_path, synthetic_path):

    random_provider = RandomProvider(SEED, MIN_TASK_POWER_DEFAULT, MAX_TASK_POWER_DEFAULT)

    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)
    photovoltaReader = PhotovoltaReader(resources_path)
    green_power = photovoltaReader.get_trace_1(size=30)


    workflows = [
        ('blast', lambda random_power: wfcommons_reader.read_blast_workflow(1000, 11.4492900609, random_power)),
       #  ('bwa', lambda random_power: wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, random_power)),
       # ('cycles', lambda random_power: wfcommons_reader.read_cycles_workflow(1000, 31.0991735531, random_power)),
       #  ('genome', lambda random_power: wfcommons_reader.read_genome_workflow(1000, 35.7812995246, random_power)),
       #  ('soykb', lambda random_power: wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, random_power)),
       #  ('srasearch', lambda random_power: wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583, random_power)),
       # ('montage', lambda random_power: wfcommons_reader.read_montage_workflow(1000, 11.17646556189, random_power)),
       #  ('seismology', lambda random_power: wfcommons_reader.read_seismology_workflow(1000, 4000, random_power)),
        #('montage_original', lambda random_power: wfcommons_reader.read_montage_workflow(60, 1, random_power)),

    ]

    cores_per_machine = 1000
    target_utilization = 0.4

    deadline_factor = 1

    boundary_strategy = BOUNDARY_DEFAULT

    algorithm_map = {
        BOUNDARY_DEFAULT: OPTION_LCB,
        BOUNDARY_LPT_PATH: OPTION_PATH_LPT,
        BOUNDARY_LPT: OPTION_LCB_LPT,
        BOUNDARY_LPT_FULL: OPTION_LCB_LPT
    }

    for name, workflow_provider in workflows:
        workflow = workflow_provider(random_provider.random_gauss)


        # critical_path_length = calc_critical_path_length(workflow)
        # machines = create_machines_with_target(workflow, critical_path_length, [cores_per_machine], target_utilization)
        # machine_count = len(machines)
        machine_count = 1

        cluster = create_cluster(machine_count, cores_per_machine, green_power, 300)
        min_makespan = estimate_min_makespan_by_algorithm(workflow, cluster.machines_list, algorithm_map[boundary_strategy])
        deadline = deadline_factor * min_makespan

        #machine_count, min_makespan_estimated = get_machines_count(workflow, cores_per_machine, target_utilization, 'default')

        print(f'{name}')
        print(f'deadline: {deadline}s')

        try:
            scheduling = bbs(workflow, deadline, 0.0, [cluster], task_sort='energy', shift_mode='none',
                             boundary_strategy=boundary_strategy)

            makespan = calc_makespan(scheduling, workflow)
            print(f'makespan: {makespan}s')
        except Exception as e:
            print('Error:', e)

        total_machine_p = machine_usage(cluster, deadline)
        total_machine_p *= 100
        print(f'machine usage: {total_machine_p:,.{2}f}%\n')

        #print(f'\t{name}\tmin_makespan: {min_makespan:,}s ({seconds_to_hours(min_makespan)})\t tasks: {len(workflow.list_of_tasks())}\t min makespan estimated: {min_makespan_estimated}\t machines: {machine_count}')
        #print( f'{min_makespan}')


def machine_usage(cluster, deadline):
    total_machine_p = 0
    for machine in cluster.machines_list:
        machine_time = machine.cores * deadline
        usage_time = machine.total_usage()

        usage_p = usage_time / machine_time
        total_machine_p += usage_p

    return total_machine_p / len(cluster.machines)

def create_cluster(machines_count, cores_per_machine, green_power, interval_size):
    machines = []

    for i in range(machines_count):
        machine_id = f'c{cores_per_machine}_m{i}'
        machines.append(
            Machine(machine_id, cores=cores_per_machine)
        )

    power_series = PowerSeries('g1', green_power, interval_size)
    cluster = Cluster('c1', power_series, machines)

    return cluster

if __name__ == '__main__':
    resources_path = '../../../../resources'
    synthetic_path = f'{resources_path}/wfcommons/synthetic'

    calc_min_makespans_of_workflows(resources_path, synthetic_path)
