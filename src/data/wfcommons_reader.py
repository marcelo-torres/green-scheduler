import json
import os
import pathlib
import random

from wfcommons import WorkflowGenerator, SrasearchRecipe, MontageRecipe, SeismologyRecipe, BlastRecipe, BwaRecipe, \
    CyclesRecipe, GenomeRecipe, SoykbRecipe

from src.scheduling.task_graph.task_graph import TaskGraph

def _generate_random_power(min, max):
    return random.uniform(min, max)


def _create_graph_from_real_trace(task_file, min_power, max_power, seed=123123123):
    with open(task_file) as f:
        data = json.load(f)

        graph = TaskGraph()

        start_task_id = '0'
        graph.add_new_task(start_task_id, runtime=0, power=0)  # Dummy task
        graph.set_start_task(start_task_id)

        task_runtimes = {}

        tasks_execution = data['workflow']['execution']['tasks']
        for task in tasks_execution:
            runtime_is_seconds = round(float(task['runtimeInSeconds']))
            task_name = task['id']
            task_runtimes[task_name] = runtime_is_seconds

        tasks = data['workflow']['specification']['tasks']

        random.seed(seed)

        for task in tasks:
            runtime_is_seconds = task_runtimes[task['name']]
            power = _generate_random_power(min_power, max_power)
            graph.add_new_task(task['name'], runtime=runtime_is_seconds, power=power)

        for task in tasks:
            current_task_name = task['name']
            parents = task['parents']
            children = task['children']

            if len(parents) == 0:
                graph.create_dependency(start_task_id, current_task_name)

            for child in children:
                graph.create_dependency(current_task_name, child)

        return graph


def _create_graph(task_file, min_power, max_power, seed=123123123):
    with open(task_file) as f:
        data = json.load(f)

        graph = TaskGraph()

        start_task_id = '0'
        graph.add_new_task(start_task_id, runtime=0, power=0)  # Dummy task
        graph.set_start_task(start_task_id)

        tasks = data['workflow']['tasks']

        random.seed(seed)

        for task in tasks:
            runtime_is_seconds = round(float(task['runtimeInSeconds']))
            if runtime_is_seconds == 0: # TODO
                runtime_is_seconds = 1
            power = _generate_random_power(min_power, max_power)
            graph.add_new_task(task['name'], runtime=runtime_is_seconds, power=power)

        for task in tasks:
            current_task_name = task['name']
            parents = task['parents']
            children = task['children']

            if len(parents) == 0:
                graph.create_dependency(start_task_id, current_task_name)

            for child in children:
                graph.create_dependency(current_task_name, child)

        return graph
class WorkflowTraceArchiveReader:

    def __init__(self, resource_path, min_power, max_power, seed=123456789):
        self.resource_path = resource_path
        self.min_power = min_power
        self.max_power = max_power
        random.seed(seed)

    def set_seed(self, seed):
        random.seed(seed)


def _get_full_name(synthetic_path, name, num_tasks, runtime_factor):
    return f'{synthetic_path}/{name}-workflow-t{num_tasks}-r{runtime_factor * 10}.json'


def _create_wfcommons_graph(workflow_name, recipe):

    if not os.path.isfile(workflow_name):
        raise Exception('Workflow file already exists')

    generator = WorkflowGenerator(recipe)
    for i, workflow in enumerate(generator.build_workflows(1)):
        workflow.write_json(pathlib.Path(workflow_name))


class WfCommonsWorkflowReader:

    MIN_TASK_POWER_DEFAULT = 1
    MAX_TASK_POWER_DEFAULT = 5

    def __init__(self, synthetic_path):
        self.synthetic_path = synthetic_path

    def create_srasearch_workflow(self, num_tasks, runtime_factor):
        self._create(SrasearchRecipe, 'srasearch', num_tasks, runtime_factor)

    def create_montage_workflow(self, num_tasks, runtime_factor):
        self._create(MontageRecipe, 'montage', num_tasks, runtime_factor)

    def create_seismology_workflow(self, num_tasks, runtime_factor):
        self._create(SeismologyRecipe, 'seismology', num_tasks, runtime_factor)

    def create_blast_workflow(self, num_tasks, runtime_factor):
        self._create(BlastRecipe, 'blast', num_tasks, runtime_factor)

    def create_bwa_workflow(self, num_tasks, runtime_factor):
        self._create(BwaRecipe, 'bwa', num_tasks, runtime_factor)

    def create_cycles_workflow(self, num_tasks, runtime_factor):
        self._create(CyclesRecipe, 'cycles', num_tasks, runtime_factor)

    def create_genome_workflow(self, num_tasks, runtime_factor):
        self._create(GenomeRecipe, 'genome', num_tasks, runtime_factor)

    def create_soykb_workflow(self, num_tasks, runtime_factor):
        self._create(SoykbRecipe, 'soykb', num_tasks, runtime_factor)

    def read_srasearch_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('srasearch', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_montage_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('montage', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_seismology_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('seismology', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_blast_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('blast', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_bwa_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('bwa', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_cycles_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('cycles', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_genome_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('genome', num_tasks, runtime_factor, min_task_power, max_task_power)

    def read_soykb_workflow(self, num_tasks, runtime_factor, min_task_power=MIN_TASK_POWER_DEFAULT, max_task_power=MAX_TASK_POWER_DEFAULT):
        return self._read('soykb', num_tasks, runtime_factor, min_task_power, max_task_power)

    def _create(self, recipe_class, workflow_name, num_tasks, runtime_factor):
        path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor)
        recipe = recipe_class.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
        _create_wfcommons_graph(path, recipe)

    def _read(self, workflow_name, num_tasks, runtime_factor, min_task_power=1, max_task_power=5):
        path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor)
        return _create_graph(path, min_task_power, max_task_power)
