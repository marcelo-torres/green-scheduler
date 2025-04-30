import json
import os
import pathlib

from wfcommons import WorkflowGenerator, SrasearchRecipe, MontageRecipe, SeismologyRecipe, BlastRecipe, BwaRecipe, \
    CyclesRecipe, GenomeRecipe, SoykbRecipe

from src.scheduling.model.task_graph import TaskGraph


def _create_graph_from_real_trace(task_file, random_power):
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

        for task in tasks:
            runtime_is_seconds = task_runtimes[task['name']]
            power = random_power()
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


def _create_graph(task_file, random_power):
    with open(task_file) as f:
        data = json.load(f)

        graph = TaskGraph()

        start_task_id = '0'
        graph.add_new_task(start_task_id, runtime=1, power=0)  # Dummy task
        graph.set_start_task(start_task_id)

        tasks = data['workflow']['tasks']

        powers_task_category = {}

        for task in tasks:
            runtime_is_seconds = round(float(task['runtimeInSeconds']))
            if runtime_is_seconds == 0: # TODO
                runtime_is_seconds = 1

            task_category = task['category']
            if task_category not in powers_task_category:
                power = random_power()
                powers_task_category[task_category] = power
            else:
                power = powers_task_category[task_category]

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

    def __init__(self, resource_path):
        self.resource_path = resource_path

def _get_full_name(synthetic_path, name, num_tasks, runtime_factor, index):
    return f'{synthetic_path}/{name}-workflow-t{num_tasks}-r{runtime_factor * 10}_i{index}.json'

def _get_full_name_legacy(synthetic_path, name, num_tasks, runtime_factor):
    return f'{synthetic_path}/{name}-workflow-t{num_tasks}-r{runtime_factor * 10}.json'


def _create_wfcommons_graph(workflow_names, recipe):

    for workflow_name in workflow_names:
        if os.path.exists(workflow_name):
            raise Exception(f'Workflow file {workflow_name} already exists')

    generator = WorkflowGenerator(recipe)
    count = len(workflow_names)
    for i, workflow in enumerate(generator.build_workflows(count)):
        workflow_name = workflow_names[i]
        workflow.write_json(
            pathlib.Path(workflow_name)
        )

def _workflow_name_i(workflow_name, i):
    return workflow_name.replace('.json', f'-{i}.json')

class WfCommonsWorkflowReader:

    MIN_TASK_POWER_DEFAULT = 1
    MAX_TASK_POWER_DEFAULT = 5

    def __init__(self, synthetic_path):
        self.synthetic_path = synthetic_path

    def create_srasearch_workflow(self, num_tasks, runtime_factor, count):
        self._create(SrasearchRecipe, 'srasearch', num_tasks, runtime_factor, count)

    def create_montage_workflow(self, num_tasks, runtime_factor, count):
        self._create(MontageRecipe, 'montage', num_tasks, runtime_factor, count)

    def create_seismology_workflow(self, num_tasks, runtime_factor, count):
        self._create(SeismologyRecipe, 'seismology', num_tasks, runtime_factor, count)

    def create_blast_workflow(self, num_tasks, runtime_factor, count):
        self._create(BlastRecipe, 'blast', num_tasks, runtime_factor, count)

    def create_bwa_workflow(self, num_tasks, runtime_factor, count):
        self._create(BwaRecipe, 'bwa', num_tasks, runtime_factor, count)

    def create_cycles_workflow(self, num_tasks, runtime_factor, count):
        self._create(CyclesRecipe, 'cycles', num_tasks, runtime_factor, count)

    def create_genome_workflow(self, num_tasks, runtime_factor, count):
        self._create(GenomeRecipe, 'genome', num_tasks, runtime_factor, count)

    def create_soykb_workflow(self, num_tasks, runtime_factor, count):
        self._create(SoykbRecipe, 'soykb', num_tasks, runtime_factor, count)

    def read_srasearch_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('srasearch', num_tasks, runtime_factor, random_power, index)

    def read_montage_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('montage', num_tasks, runtime_factor, random_power, index)

    def read_seismology_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('seismology', num_tasks, runtime_factor, random_power, index)

    def read_blast_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('blast', num_tasks, runtime_factor, random_power, index)

    def read_bwa_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('bwa', num_tasks, runtime_factor, random_power, index)

    def read_cycles_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('cycles', num_tasks, runtime_factor, random_power, index)

    def read_genome_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('genome', num_tasks, runtime_factor, random_power, index)

    def read_soykb_workflow(self, num_tasks, runtime_factor, random_power, index=None):
        return self._read('soykb', num_tasks, runtime_factor, random_power, index)

    def delete_srasearch_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('srasearch', num_tasks, runtime_factor, index)

    def delete_montage_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('montage', num_tasks, runtime_factor, index)

    def delete_seismology_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('seismology', num_tasks, runtime_factor, index)

    def delete_blast_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('blast', num_tasks, runtime_factor, index)

    def delete_bwa_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('bwa', num_tasks, runtime_factor, index)

    def delete_cycles_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('cycles', num_tasks, runtime_factor, index)

    def delete_genome_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('genome', num_tasks, runtime_factor, index)

    def delete_soykb_workflow(self, num_tasks, runtime_factor, index=None):
        self._delete('soykb', num_tasks, runtime_factor, index)

    def change_index_srasearch_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('srasearch', num_tasks, runtime_factor, index, new_index)

    def change_index_montage_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('montage', num_tasks, runtime_factor, index, new_index)

    def change_index_seismology_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('seismology', num_tasks, runtime_factor, index, new_index)

    def change_index_blast_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('blast', num_tasks, runtime_factor, index, new_index)

    def change_index_bwa_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('bwa', num_tasks, runtime_factor, index, new_index)

    def change_index_cycles_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('cycles', num_tasks, runtime_factor, index, new_index)

    def change_index_genome_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('genome', num_tasks, runtime_factor, index, new_index)

    def change_index_soykb_workflow(self, num_tasks, runtime_factor, index, new_index):
        self._change_index('soykb', num_tasks, runtime_factor, index, new_index)

    def _create(self, recipe_class, workflow_name, num_tasks, runtime_factor, count):

        paths = []
        for i in range(count):
            path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor, i)
            paths.append(path)

        recipe = recipe_class.from_num_tasks(num_tasks, runtime_factor=runtime_factor)
        _create_wfcommons_graph(paths, recipe)

    def _read(self, workflow_name, num_tasks, runtime_factor, random_power, index):

        if index is None:
            path = _get_full_name_legacy(self.synthetic_path, workflow_name, num_tasks, runtime_factor)
        else:
            path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor, index)
        return _create_graph(path, random_power)

    def _delete(self, workflow_name, num_tasks, runtime_factor, index):
        if index is None:
            path = _get_full_name_legacy(self.synthetic_path, workflow_name, num_tasks, runtime_factor)
        else:
            path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor, index)
            
        os.remove(path)
        
    def _change_index(self, workflow_name, num_tasks, runtime_factor, index, new_index):
        old_path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor, index)
        new_path = _get_full_name(self.synthetic_path, workflow_name, num_tasks, runtime_factor, new_index)
        
        os.rename(old_path, new_path)
        