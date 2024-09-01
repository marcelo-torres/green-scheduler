from src.scheduling.algorithms.highest_power_first.boundaries.constant_boundary import calculate_left_boundary, \
    calculate_right_boundary
from src.scheduling.util.calc_levels import calc_levels


class BoundaryCalculator:

    def __init__(self, graph, deadline, c):
        task_levels, max_level = calc_levels(graph)
        self.task_levels = task_levels
        self.max_level = max_level
        self.deadline = deadline
        self.c = c

    def calculate_boundaries(self, task, scheduling):
        lcb, is_limited_by_scheduled_predecessor = calculate_left_boundary(task, scheduling)
        rcb, is_limited_by_scheduled_successor = calculate_right_boundary(task, scheduling, self.deadline)

        available_time = abs(self.deadline - lcb - rcb)
        available_time_to_use = round((1-self.c) * available_time)
        time_to_variable_boundary = abs(available_time_to_use - available_time)

        # Todo review this logic
        # if available_time_to_use < task.runtime:
        #    available_time_to_use += task.runtime-available_time_to_use
        #    time_to_variable_boundary = abs(available_time_to_use - available_time)

        task_level = float(self.task_levels[task.id])
        left_c = task_level / self.max_level
        right_c = 1 - left_c

        #lvb = available_time_to_use * left_c
        #rvb = available_time_to_use * right_c

        lvb = round(time_to_variable_boundary * left_c)
        rvb = time_to_variable_boundary - lvb

        if lcb == 0 or is_limited_by_scheduled_predecessor:
            lvb = 0

        if rcb == 0 or is_limited_by_scheduled_successor:
            rvb = 0

        if self.deadline - lcb - lvb - rcb - rvb < task.runtime:
            lvb = 0
            rvb = 0

        return lcb, lvb, rcb, rvb
