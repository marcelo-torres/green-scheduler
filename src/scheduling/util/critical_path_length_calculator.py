from src.scheduling.util.calc_levels import calc_levels
from src.scheduling.task_graph.task_graph import TaskGraph
from src.scheduling.util.topological_ordering import sort_topologically


def calc_critical_path_length(graph):

    # print(
    #     f'isCyclic: {isCyclic(graph)}'
    # )

    tasks = sort_topologically(graph)

    start_times = {graph.get_first_task().id: 0}

    # Earliest start time computation
    for task_id in tasks:
        task = graph.get_task(task_id)
        start_time = start_times[task.id] if task.id in start_times else 0
        finish_time = start_time + task.runtime
        for child in task.successors:
            if child.id not in start_times or finish_time > start_times[child.id]:
                start_times[child.id] = finish_time

    slacks = {}
    # Calc slack time
    for task_id in tasks:
        task = graph.get_task(task_id)
        if len(task.successors) == 0:
            slacks[task.id] = 0

        else:
            min_children_start_time = float('inf')
            for child in task.successors:
                if start_times[child.id] < min_children_start_time:
                    min_children_start_time = start_times[child.id]

            slacks[task.id] = min_children_start_time - start_times[task.id] - task.runtime

    # If there are two parallel paths withs tasks with the same runtime, then both paths can be critical.
    # Get only one task with slack time = 0 per critical path
    levels, max_level = calc_levels(graph)
    max_of_level = {}
    for task_id, level in levels.items():
        task = graph.get_task(task_id)
        if slacks[task_id] == 0:
            if level not in max_of_level:
                max_of_level[level] = task.runtime
            else:
                current_max = max_of_level[level]
                if task.runtime > current_max:
                    max_of_level[level] = task.runtime

    critical_path_length = 0
    for level, max_runtime in max_of_level.items():
        critical_path_length += max_runtime

    return critical_path_length

def isCyclicUtil(task, visited, recStack):

    # Mark current node as visited and
    # adds to recursion stack
    visited[task.id] = True
    recStack[task.id] = True

    # Recur for all neighbours
    # if any neighbour is visited and in
    # recStack then graph is cyclic
    for child in task.successors:
        if not visited[child.id]:
            if isCyclicUtil(child, visited, recStack):
                return True
        elif recStack[child.id]:
            return True

    # The node needs to be popped from
    # recursion stack before function ends
    recStack[task.id] = False
    return False

    # Returns true if graph is cyclic else false
def isCyclic(graph):
    tasks = graph.list_of_tasks()

    visited = {}
    rec_stack = {}

    for task in tasks:
        visited[task.id] = False
        rec_stack[task.id] = False

    for task in tasks:
        if not visited[task.id]:
            if isCyclicUtil(task, visited, rec_stack):
                return True
    return False

if __name__ == '__main__':
    def create_graph():
        graph = TaskGraph()
        graph.set_start_task(0)
        graph.add_new_task(0, runtime=0, power=0)  # Dummy task
        graph.add_new_task(1, runtime=10, power=14)
        graph.add_new_task(2, runtime=15, power=10)
        graph.add_new_task(3, runtime=20, power=12)
        graph.add_new_task(4, runtime=7, power=18)
        graph.add_new_task(5, runtime=14, power=14)
        graph.add_new_task(6, runtime=12, power=16)
        graph.add_new_task(7, runtime=8, power=4)

        graph.create_dependency(0, 1)
        graph.create_dependency(0, 2)
        graph.create_dependency(1, 3)
        graph.create_dependency(2, 3)
        graph.create_dependency(3, 6)
        graph.create_dependency(3, 4)
        graph.create_dependency(3, 5)
        graph.create_dependency(4, 6)
        graph.create_dependency(4, 7)
        graph.create_dependency(5, 7)
        graph.create_dependency(6, 7)

        return graph


    graph = create_graph()

    critical_path_length = calc_critical_path_length(graph)
    print(critical_path_length)

    assert critical_path_length == 62