import json
import random

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
