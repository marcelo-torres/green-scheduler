import os

from wfcommons import WorkflowGenerator, BlastRecipe, MontageRecipe

from src.data.wfcommons_reader import _create_graph
from src.scheduling.drawer.task_graph_drawer import draw_task_graph


def _workflow_to_graph(workflow):

    path = 'temp.json'

    workflow.write_json(path)
    graph = _create_graph(path, lambda: 1)

    os.remove(path)

    return graph

def _sum_runtime(tasks):
    runtime_sum = 0

    for task in tasks:
        runtime_sum += task.runtime

    return runtime_sum


if __name__ == "__main__":
    #generator = WorkflowGenerator(BlastRecipe.from_num_tasks(45))
    generator = WorkflowGenerator(BlastRecipe.from_num_tasks(50, runtime_factor=2))
    #generator = WorkflowGenerator(MontageRecipe.from_num_tasks(60))
    #generator = WorkflowGenerator(MontageRecipe.from_num_tasks(80))
    workflows = generator.build_workflows(10)

    for i, workflow in enumerate(generator.build_workflows(10)): # TODO
        print(i)

    for workflow in workflows:
        graph = _workflow_to_graph(workflow)

        draw_task_graph(graph)

        tasks = graph.list_of_tasks()
        runtime_sum =  _sum_runtime(tasks)

        print(f'tasks: {len(tasks)}\truntime_sum: {runtime_sum}s')
