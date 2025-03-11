import os
from pathlib import Path

from src.data.wfcommons_reader import WfCommonsWorkflowReader

PATH = base_dir = f'{Path(__file__).parent}/../../../resources/sanity_test_workflows/'
NUM_TASKS = 110
RUNTIME_FACTOR = 1


def _init_workflows():
    reader = WfCommonsWorkflowReader(PATH)

    reader.create_srasearch_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_montage_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_seismology_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_blast_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_bwa_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_cycles_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_genome_workflow(NUM_TASKS, RUNTIME_FACTOR)
    reader.create_soykb_workflow(NUM_TASKS, RUNTIME_FACTOR)


def load_workflows():

    if not _are_workflows_generated():
        raise Exception('You first need to generate workflows for sanity test!')

    wfcommons_reader = WfCommonsWorkflowReader(PATH)

    workflow_providers = [
        ('blast', lambda random_power: wfcommons_reader.read_blast_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('bwa', lambda random_power: wfcommons_reader.read_bwa_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('cycles', lambda random_power: wfcommons_reader.read_cycles_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('genome', lambda random_power: wfcommons_reader.read_genome_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('soykb', lambda random_power: wfcommons_reader.read_soykb_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('srasearch', lambda random_power: wfcommons_reader.read_srasearch_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('montage', lambda random_power: wfcommons_reader.read_montage_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
        ('seismology', lambda random_power: wfcommons_reader.read_seismology_workflow(NUM_TASKS, RUNTIME_FACTOR, random_power)),
    ]

    return workflow_providers


def _are_workflows_generated():
    if os.path.exists(PATH) and not os.path.isfile(PATH):
        dir = os.listdir(PATH)
        return len(dir) > 0
    return False


if __name__ == '__main__':
    if not _are_workflows_generated():
        print("Generating workflows for sanity check")
        _init_workflows()
    else:
        print('Skipping workflows generation for sanity test')