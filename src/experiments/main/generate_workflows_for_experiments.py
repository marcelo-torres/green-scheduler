import math

from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_estimator import estimate_min_makespan_with_enough_cores
from src.scheduling.util.max_concurrent_tasks import count_max_parallel_tasks

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

NUM_OF_TASKS = 1000

workflow_map = {
    'blast': {
        'reader': lambda reader: reader.read_blast_workflow,
        'change_index': lambda reader: reader.change_index_blast_workflow,
        'delete': lambda reader: reader.delete_blast_workflow,
    },
    'bwa': {
        'reader': lambda reader: reader.read_bwa_workflow,
        'change_index': lambda reader: reader.change_index_bwa_workflow,
        'delete': lambda reader: reader.delete_bwa_workflow,
    },
    'cycles': {
        'reader': lambda reader: reader.read_cycles_workflow,
        'delete': lambda reader: reader.delete_cycles_workflow,
        'change_index': lambda reader: reader.change_index_cycles_workflow,
    },
    'genome': {
        'reader': lambda reader: reader.read_genome_workflow,
        'delete': lambda reader: reader.delete_genome_workflow,
        'change_index': lambda reader: reader.change_index_genome_workflow,
    },
    'soykb': {
        'reader': lambda reader: reader.read_soykb_workflow,
        'delete': lambda reader: reader.delete_soykb_workflow,
        'change_index': lambda reader: reader.change_index_soykb_workflow,
    },
    'srasearch': {
        'reader': lambda reader: reader.read_srasearch_workflow,
        'delete': lambda reader: reader.delete_srasearch_workflow,
        'change_index': lambda reader: reader.change_index_srasearch_workflow,
    },
    'montage': {
        'reader': lambda reader: reader.read_montage_workflow,
        'delete': lambda reader: reader.delete_montage_workflow,
        'change_index': lambda reader: reader.change_index_montage_workflow,
    },
    'seismology': {
        'reader': lambda reader: reader.read_seismology_workflow,
        'delete': lambda reader: reader.delete_seismology_workflow,
        'change_index': lambda reader: reader.change_index_seismology_workflow,
    },
}

num_of_tasks_bigger = 1000
runtime_factor_map_bigger = {
    'blast': 11.4492900609,
    'bwa': 52.2248138958,
    'cycles': 31.0991735531,
    'genome': 35.7812995246,
    'soykb': 3.85224364443,
    'srasearch': 1.26845637583,
    'montage': 11.17646556189,
    'seismology': 4000,
}

num_of_tasks_smaller = 200
runtime_factor_map_smaller = {
    'blast': runtime_factor_map_bigger['blast'] / 5,
    'bwa': runtime_factor_map_bigger['bwa'] / 5,
    'cycles': runtime_factor_map_bigger['cycles'] / 5,
    'genome': runtime_factor_map_bigger['genome'] / 5,
    'soykb': runtime_factor_map_bigger['soykb'] / 5,
    'srasearch': runtime_factor_map_bigger['srasearch'] / 5,
    'montage': runtime_factor_map_bigger['montage'] / 5,
    'seismology': runtime_factor_map_bigger['seismology'] / 5,
}

def constant_power():
    return 1

def _generate_workflows(wfcommons_reader, count, num_of_tasks, runtime_factor_map):
    wfcommons_reader.create_blast_workflow(num_of_tasks, runtime_factor_map['blast'], count)
    wfcommons_reader.create_bwa_workflow(num_of_tasks, runtime_factor_map['bwa'], count)
    wfcommons_reader.create_cycles_workflow(num_of_tasks, runtime_factor_map['cycles'], count)
    wfcommons_reader.create_genome_workflow(num_of_tasks, runtime_factor_map['genome'], count)
    wfcommons_reader.create_soykb_workflow(num_of_tasks, runtime_factor_map['soykb'], count)
    wfcommons_reader.create_srasearch_workflow(num_of_tasks, runtime_factor_map['srasearch'], count)
    wfcommons_reader.create_montage_workflow(num_of_tasks, runtime_factor_map['montage'], count)
    wfcommons_reader.create_seismology_workflow(num_of_tasks, runtime_factor_map['seismology'], count)


def _filter_workflows(wfcommons_reader, count, count_to_select, num_of_tasks, runtime_factor_map):

    assert count >= count_to_select
    
    for name, workflow_data in workflow_map.items():

        runtime_factor = runtime_factor_map[name]

        workflows_stats = _get_critical_path_length_of(wfcommons_reader, workflow_data, count, num_of_tasks, runtime_factor)
        workflows_stats.sort(key=lambda x: x[1])
        selected_workflows, exclude_list = _select_middle_elements(workflows_stats, count, count_to_select)
        _log_delete_list(name, exclude_list)
        _delete_workflows_in(exclude_list, wfcommons_reader, workflow_data, num_of_tasks, runtime_factor)
        _rename_workflows_in(selected_workflows, wfcommons_reader, workflow_data, num_of_tasks, runtime_factor)

def _get_critical_path_length_of(wfcommons_reader, workflow_data, count, num_of_tasks, runtime_factor):
    workflows_stats = []

    workflow_provider = workflow_data['reader'](wfcommons_reader)
    for index in range(count):
        workflow = workflow_provider(num_of_tasks, runtime_factor, constant_power, index)
        critical_path_length = calc_critical_path_length(workflow)

        workflows_stats.append(
            (index, critical_path_length)
        )

    return workflows_stats

def _select_middle_elements(list_of_elements, count, count_to_select):
    assert count >= count_to_select

    start = math.floor(
        (count-count_to_select) / 2
    )
    end = start + count_to_select

    exclude_list = list_of_elements[:start] + list_of_elements[end:]

    return list_of_elements[start:end], exclude_list

def _log_delete_list(workflow_name, exclude_list):
    print(f'Delete list\t{workflow_name}', end='\t')
    for index, calc_critical_path_length in exclude_list:
        print(f'{calc_critical_path_length}', end='\t')
    print()

def _delete_workflows_in(exclude_list, wfcommons_reader, workflow_data, num_of_tasks, runtime_factor):
    for index, _ in exclude_list:
        workflow_data['delete'](wfcommons_reader)(num_of_tasks, runtime_factor, index)

def _rename_workflows_in(selected_workflows, wfcommons_reader, workflow_data, num_of_tasks, runtime_factor):

    selected_workflows.sort(key=lambda x: x[0])

    new_index = 0
    for index, _ in selected_workflows:
        workflow_data['change_index'](wfcommons_reader)(num_of_tasks, runtime_factor, index, new_index)
        new_index += 1



def show_stats(wfcommons_reader, count, workflow_map, num_of_tasks, runtime_factor_map):

    separator = '\t'

    headers = ['workflow', 'index', 'tasks', 'max_parallel_tasks', 'min_makespan_lpt']

    print(separator.join(headers))

    for name, workflow_data in workflow_map.items():
        workflow_provider = workflow_data['reader'](wfcommons_reader)
        runtime_factor = runtime_factor_map[name]
        for i in range(count):
            workflow = workflow_provider(num_of_tasks, runtime_factor, constant_power, i)

            print(name, end=separator)
            print(i, end=separator)
            print(len(workflow.list_of_tasks()), end=separator)
            print(count_max_parallel_tasks(workflow), end=separator)
            print(estimate_min_makespan_with_enough_cores(workflow))


def generate_workflows_for_experiments(count, count_to_select, num_of_tasks, runtime_factor_map):
    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)

    #_generate_workflows(wfcommons_reader, count, num_of_tasks, runtime_factor_map)
    _filter_workflows(wfcommons_reader, count, count_to_select, num_of_tasks, runtime_factor_map)
    show_stats(wfcommons_reader, count_to_select, workflow_map, num_of_tasks, runtime_factor_map)

if __name__ == '__main__':
    count = 100
    count_to_select = 10

    #generate_workflows_for_experiments(count, count_to_select, num_of_tasks_bigger, runtime_factor_map_bigger)
    generate_workflows_for_experiments(count, count_to_select, num_of_tasks_smaller, runtime_factor_map_smaller)

