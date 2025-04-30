import math

from pycrunch.child_runtime.exclusions import exclude_list

from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_estimator import estimate_min_makespan_with_enough_cores
from src.scheduling.util.max_concurrent_tasks import count_max_parallel_tasks

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

workflow_map = {
    'blast': {
        'tasks': 1000,
        'runtime_factor': 11.4492900609,
        'reader': lambda reader: reader.read_blast_workflow,
        'change_index': lambda reader: reader.change_index_blast_workflow,
        'delete': lambda reader: reader.delete_blast_workflow,
    },
    'bwa': {
        'tasks': 1000,
        'runtime_factor': 52.2248138958,
        'reader': lambda reader: reader.read_bwa_workflow,
        'change_index': lambda reader: reader.change_index_bwa_workflow,
        'delete': lambda reader: reader.delete_bwa_workflow,
    },
    'cycles': {
        'tasks': 1000,
        'runtime_factor': 31.0991735531,
        'reader': lambda reader: reader.read_cycles_workflow,
        'delete': lambda reader: reader.delete_cycles_workflow,
        'change_index': lambda reader: reader.change_index_cycles_workflow,
    },
    'genome': {
        'tasks': 1000,
        'runtime_factor': 35.7812995246,
        'reader': lambda reader: reader.read_genome_workflow,
        'delete': lambda reader: reader.delete_genome_workflow,
        'change_index': lambda reader: reader.change_index_genome_workflow,
    },
    'soykb': {
        'tasks': 1000,
        'runtime_factor': 3.85224364443,
        'reader': lambda reader: reader.read_soykb_workflow,
        'delete': lambda reader: reader.delete_soykb_workflow,
        'change_index': lambda reader: reader.change_index_soykb_workflow,
    },
    'srasearch': {
        'tasks': 1000,
        'runtime_factor': 1.26845637583,
        'reader': lambda reader: reader.read_srasearch_workflow,
        'delete': lambda reader: reader.delete_srasearch_workflow,
        'change_index': lambda reader: reader.change_index_srasearch_workflow,
    },
    'montage': {
        'tasks': 1000,
        'runtime_factor': 11.17646556189,
        'reader': lambda reader: reader.read_montage_workflow,
        'delete': lambda reader: reader.delete_montage_workflow,
        'change_index': lambda reader: reader.change_index_montage_workflow,
    },
    'seismology': {
        'tasks': 1000,
        'runtime_factor': 4000,
        'reader': lambda reader: reader.read_seismology_workflow,
        'delete': lambda reader: reader.delete_seismology_workflow,
        'change_index': lambda reader: reader.change_index_seismology_workflow,
    },
}

def constant_power():
    return 1

def generate_workflows(wfcommons_reader, count):
    wfcommons_reader.create_blast_workflow(1000, 11.4492900609, count)
    wfcommons_reader.create_bwa_workflow(1000, 52.2248138958, count)
    wfcommons_reader.create_cycles_workflow(1000, 31.0991735531, count)
    wfcommons_reader.create_genome_workflow(1000, 35.7812995246, count)
    wfcommons_reader.create_soykb_workflow(1000, 3.85224364443, count)
    wfcommons_reader.create_srasearch_workflow(1000, 1.26845637583, count)
    wfcommons_reader.create_montage_workflow(1000, 11.17646556189, count)
    wfcommons_reader.create_seismology_workflow(1000, 4000, count)

def filter_workflows(wfcommons_reader, count, count_to_select):

    assert count >= count_to_select
    
    for name, workflow_data in workflow_map.items():

        workflows_stats = _get_critical_path_length_of(wfcommons_reader, workflow_data, count)
        workflows_stats.sort(key=lambda x: x[1])
        selected_workflows, exclude_list = select_middle_elements(workflows_stats, count, count_to_select)
        _log_delete_list(name, exclude_list)
        _delete_workflows_in(exclude_list, wfcommons_reader, workflow_data)
        _rename_workflows_in(selected_workflows, wfcommons_reader, workflow_data)

def _get_critical_path_length_of(wfcommons_reader, workflow_data, count):
    workflows_stats = []

    workflow_provider = workflow_data['reader'](wfcommons_reader)
    for index in range(count):
        workflow = workflow_provider(workflow_data['tasks'], workflow_data['runtime_factor'], constant_power, index)
        critical_path_length = calc_critical_path_length(workflow)

        workflows_stats.append(
            (index, critical_path_length)
        )

    return workflows_stats

def select_middle_elements(list_of_elements, count, count_to_select):
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

def _delete_workflows_in(exclude_list, wfcommons_reader, workflow_data):
    for index, _ in exclude_list:
        workflow_data['delete'](wfcommons_reader)(workflow_data['tasks'], workflow_data['runtime_factor'], index)

def _rename_workflows_in(selected_workflows, wfcommons_reader, workflow_data):

    selected_workflows.sort(key=lambda x: x[0])

    new_index = 0
    for index, _ in selected_workflows:
        workflow_data['change_index'](wfcommons_reader)(workflow_data['tasks'], workflow_data['runtime_factor'], index, new_index)
        new_index += 1



def show_stats(wfcommons_reader, count):
    workflow_providers = [
        ('blast', lambda index: wfcommons_reader.read_blast_workflow(1000, 11.4492900609, lambda: 1, index)),
        ('bwa', lambda index: wfcommons_reader.read_bwa_workflow(1000, 52.2248138958, lambda: 1, index)),
        ('cycles', lambda index: wfcommons_reader.read_cycles_workflow(1000, 31.0991735531, lambda: 1, index)),
        ('genome', lambda index: wfcommons_reader.read_genome_workflow(1000, 35.7812995246, lambda: 1, index)),
        ('soykb', lambda index: wfcommons_reader.read_soykb_workflow(1000, 3.85224364443, lambda: 1, index)),
        ('srasearch', lambda index: wfcommons_reader.read_srasearch_workflow(1000, 1.26845637583, lambda: 1, index)),
        ('montage', lambda index: wfcommons_reader.read_montage_workflow(1000, 11.17646556189, lambda: 1, index)),
        ('seismology', lambda index: wfcommons_reader.read_seismology_workflow(1000, 4000, lambda: 1, index)),
    ]

    separator = '\t'

    headers = ['workflow', 'index', 'tasks', 'max_parallel_tasks', 'critical_path_length', 'min_makespan_lpt']

    print(separator.join(headers))

    for name, workflow_provider in workflow_providers:
        for i in range(count):
            workflow = workflow_provider(i)

            print(name, end=separator)
            print(i, end=separator)
            print(len(workflow.list_of_tasks()), end=separator)
            print(count_max_parallel_tasks(workflow), end=separator)
            print(calc_critical_path_length(workflow), end=separator)
            print(estimate_min_makespan_with_enough_cores(workflow))


if __name__ == '__main__':
    count = 50
    count_to_select = 10

    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)

    generate_workflows(wfcommons_reader, count)
    filter_workflows(wfcommons_reader, count, count_to_select)
    show_stats(wfcommons_reader, count_to_select)
