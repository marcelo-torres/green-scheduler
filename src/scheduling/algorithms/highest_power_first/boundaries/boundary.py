from src.scheduling.algorithms.highest_power_first.boundaries.constant_boundary import calculate_left_boundary, \
    calculate_right_boundary
from src.scheduling.algorithms.highest_power_first.calc_levels import calc_levels


class BoundaryCalculator:

    def __init__(self, graph, deadline, c):
        task_levels, max_level = calc_levels(graph)
        self.task_levels = task_levels
        self.max_level = max_level
        self.deadline = deadline
        self.c = c


    def calculate_boundaries(self, task, scheduling):
        lcb = calculate_left_boundary(task, scheduling)
        rcb = calculate_right_boundary(task, scheduling)

        available_time = self.deadline - lcb - rcb
        available_time_to_use = round(self.c * available_time)

        task_level = float(self.task_levels[task.id])
        left_c = task_level / self.max_level
        right_c = 1 - left_c

        #lvb = available_time_to_use * left_c
        #rvb = available_time_to_use * right_c

        lvb = round(available_time_to_use * left_c)
        rvb = available_time_to_use - lvb

        if lcb == 0:
            lvb = 0

        if rcb == 0:
            rvb = 0

        print(f'Task #{task} [{lcb}] {lvb} ({available_time-rvb-lvb}) {rvb} [{rcb}]')

        return lcb, lvb, rcb, rvb

        # lb = lcb + lvb
        # rb = rcb + rvb



        #return lb, rb
