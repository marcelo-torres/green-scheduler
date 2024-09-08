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
