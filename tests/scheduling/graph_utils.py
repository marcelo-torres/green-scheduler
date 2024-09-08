from src.scheduling.task_graph.task_graph import TaskGraph


def get_pipeline_graph():
    """
        1 -> 2 -> 3 -> 4

    :return: TaskGraph
    """

    graph = TaskGraph()
    task_1 = graph.add_new_task(1, 4, 8)
    graph.set_start_task(task_1.id)

    task_2 = graph.add_new_task(2, 10, 5)
    graph.create_dependency(task_1.id, task_2.id)

    task_3 = graph.add_new_task(3, 7, 5)
    graph.create_dependency(task_2.id, task_3.id)

    task_4 = graph.add_new_task(4, 9, 3)
    graph.create_dependency(task_3.id, task_4.id)

    return graph


def get_parallel_graph():
    """
           1
         /  \
        2    3
        |   / \
        4  5   6
        \  |  /
         \ | /
           7

    :return: TaskGraph
    """

    graph = TaskGraph()

    # Level 0
    task_1 = graph.add_new_task(1, 4, 8)
    graph.set_start_task(task_1.id)

    # Level 1
    task_2 = graph.add_new_task(2, 10, 5)
    graph.create_dependency(task_1.id, task_2.id)

    task_3 = graph.add_new_task(3, 7, 5)
    graph.create_dependency(task_1.id, task_3.id)

    # Level 2
    task_4 = graph.add_new_task(4, 3, 6)
    graph.create_dependency(task_2.id, task_4.id)

    task_5 = graph.add_new_task(5, 2, 3)
    graph.create_dependency(task_3.id, task_5.id)

    task_6 = graph.add_new_task(6, 7, 8)
    graph.create_dependency(task_3.id, task_6.id)

    # Level 3
    task_7 = graph.add_new_task(7, 2, 2)
    graph.create_dependency(task_4.id, task_7.id)
    graph.create_dependency(task_5.id, task_7.id)
    graph.create_dependency(task_6.id, task_7.id)

    return graph


def get_stencil_graph():
    """
            01
          / | \
        02 03 04
         x  x  x
        05 06 07
         x  x  x
        08 09 10
         \ | /
          11

    :return: TaskGraph
    """

    graph = TaskGraph()

    # Level 0
    task_1 = graph.add_new_task(1, 4, 8)
    graph.set_start_task(task_1.id)

    # Level 1
    task_2 = graph.add_new_task(2, 10, 19)
    task_3 = graph.add_new_task(3, 7, 5)
    task_4 = graph.add_new_task(4, 8, 15)

    tasks_level_1 = (task_2, task_3, task_4)

    for task in tasks_level_1:
        graph.create_dependency(task_1.id, task.id)

    # Level 2
    task_5 = graph.add_new_task(5, 3, 5)
    task_6 = graph.add_new_task(6, 6, 6)
    task_7 = graph.add_new_task(7, 7, 1)

    tasks_level_2 = (task_5, task_6, task_7)

    for parent in tasks_level_1:
        for child in tasks_level_2:
            graph.create_dependency(parent.id, child.id)

    # Level 3
    task_8 = graph.add_new_task(8, 3, 1)
    task_9 = graph.add_new_task(9, 1, 4)
    task_10 = graph.add_new_task(10, 2, 7)

    tasks_level_3 = (task_8, task_9, task_10)

    for parent in tasks_level_2:
        for child in tasks_level_3:
            graph.create_dependency(parent.id, child.id)

    # Level 4
    task_11 = graph.add_new_task(11, 15, 8)
    for parent in tasks_level_3:
        graph.create_dependency(parent.id, task_11.id)

    return graph

def get_multidependency_graph():
    """
          1
         / \
        /\  \
        2 3 |
        \/  |
         \ /
          4
          |
          5
         /\
        6 7
        | |
        8 9
        \/
        10

    :return: TaskGraph
    """

    graph = TaskGraph()

    # Level 0
    task_1 = graph.add_new_task(1, 4, 8)
    graph.set_start_task(task_1.id)

    # Level 1
    task_2 = graph.add_new_task(2, 10, 19)
    task_3 = graph.add_new_task(3, 7, 5)

    graph.create_dependency(task_1.id, task_2.id)
    graph.create_dependency(task_1.id, task_3.id)

    # Level 2
    task_4 = graph.add_new_task(4, 8, 15)
    graph.create_dependency(task_1.id, task_4.id)
    graph.create_dependency(task_2.id, task_4.id)
    graph.create_dependency(task_3.id, task_4.id)

    # Level 3
    task_5 = graph.add_new_task(5, 10, 5)
    graph.create_dependency(task_4.id, task_5.id)

    # Level 4
    task_6 = graph.add_new_task(6, 6, 6)
    task_7 = graph.add_new_task(7, 7, 1)

    graph.create_dependency(task_5.id, task_6.id)
    graph.create_dependency(task_5.id, task_7.id)

    # Level 5
    task_8 = graph.add_new_task(8, 10, 1)
    task_9 = graph.add_new_task(9, 1, 4)

    graph.create_dependency(task_6.id, task_8.id)
    graph.create_dependency(task_7.id, task_9.id)

    # Level 11
    task_10 = graph.add_new_task(10, 2, 7)

    graph.create_dependency(task_8.id, task_10.id)
    graph.create_dependency(task_9.id, task_10.id)

    return graph
