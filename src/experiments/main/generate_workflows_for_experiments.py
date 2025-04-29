from src.data.wfcommons_reader import WfCommonsWorkflowReader
from src.scheduling.util.critical_path_length_calculator import calc_critical_path_length
from src.scheduling.util.makespan_estimator import estimate_min_makespan_with_enough_cores
from src.scheduling.util.max_concurrent_tasks import count_max_parallel_tasks

resources_path = '../../../resources'
synthetic_path = f'{resources_path}/wfcommons/synthetic'

def generate_workflows(wfcommons_reader, count):
    wfcommons_reader.create_blast_workflow(1000, 11.4492900609, count)
    wfcommons_reader.create_bwa_workflow(1000, 52.2248138958, count)
    wfcommons_reader.create_cycles_workflow(1000, 31.0991735531, count)
    wfcommons_reader.create_genome_workflow(1000, 35.7812995246, count)
    wfcommons_reader.create_soykb_workflow(1000, 3.85224364443, count)
    wfcommons_reader.create_srasearch_workflow(1000, 1.26845637583, count)
    wfcommons_reader.create_montage_workflow(1000, 11.17646556189, count)
    wfcommons_reader.create_seismology_workflow(1000, 4000, count)


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
    count = 10

    wfcommons_reader = WfCommonsWorkflowReader(synthetic_path)

    generate_workflows(wfcommons_reader, count)
    show_stats(wfcommons_reader, count)
