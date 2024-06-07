import matplotlib

from src.scheduling_algorithms.highest_power_first.boundaries.boundary import BoundaryCalculator
from src.scheduling_algorithms.highest_power_first.drawer import Drawer
from src.task_graph.task_graph import TaskGraph




def create_graph():
    graph = TaskGraph()
    graph.set_start_task(0)
    graph.add_new_task(0, runtime=0, power=0) # Dummy task
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

def schedule_graph(graph, deadline, c=0.5):

    schedule = {}

    tasks = graph.list_of_tasks()

    # 1) Order all tasks by energy usage (power * runtime)
    tasks.sort(key=lambda t: t.power * t.runtime, reverse=True)

    boundary_calc = BoundaryCalculator(graph, deadline, c)

    for task in tasks:

        # 2.1)  Calculate boundaries to avoid that a single task gets all slack time
        lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, schedule)

        lb = lcb + lvb
        rb = rcb + rvb

        # 2.2) Schedule each task when it uses less brown energy as early as possible

        interval_size = 10
        green_energy = [2, 7, 10, 18, 23, 27, 30, 27, 24, 21, 18, 14]

        schedule[task.id] = lb # TMP - schedule as early as possible

        drawer = Drawer()
        drawer.add_constant_boundary(0, lcb)
        drawer.add_variable_boundary(lcb, lvb)
        drawer.add_variable_boundary(deadline-rvb-rcb, rvb)
        drawer.add_constant_boundary(deadline-rcb, rcb)

        interval_start = 0
        for power in green_energy:
            drawer.add_green_energy_availability(interval_start, interval_size, power)
            interval_start += interval_size

        for task_id, start_time in schedule.items():
            drawer.add_scheduled_task(start_time, graph.get_task(task_id).runtime)

        drawer.add_task(schedule[task.id], task.runtime)
        drawer.show()

G = create_graph()
schedule_graph(G, 124)

