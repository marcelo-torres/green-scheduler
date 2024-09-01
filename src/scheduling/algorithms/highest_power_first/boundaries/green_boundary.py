from src.scheduling.algorithms.highest_power_first.boundaries.constant_boundary import calculate_right_boundary, \
    calculate_left_boundary
from src.scheduling.util.calc_levels import calc_levels


class GreenBoundaryCalculator:
    def __init__(self, graph, deadline, c):
        task_levels, max_level = calc_levels(graph)
        self.task_levels = task_levels
        self.max_level = max_level
        self.deadline = deadline
        self.c = c

    def calculate_boundaries(self, task, scheduling):
        lcb, is_limited_by_scheduled_predecessor = calculate_left_boundary(task, scheduling)
        rcb, is_limited_by_scheduled_successor = calculate_right_boundary(task, scheduling, self.deadline)

        available_time = self.deadline - lcb - rcb

        # TODO

        if lcb == 0 or is_limited_by_scheduled_predecessor:
            lvb = 0

        if rcb == 0 or is_limited_by_scheduled_successor:
            rvb = 0

        return lcb, lvb, rcb, rvb