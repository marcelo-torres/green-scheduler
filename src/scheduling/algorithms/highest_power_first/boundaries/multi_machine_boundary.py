from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine_constant_left_boundary import \
    calculate_constant_left_boundary
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine_constant_right_boundary import \
    calculate_constant_right_boundary
from src.scheduling.util.calc_levels import calc_levels


class MultiMachineBoundaryCalculator:

    def __init__(self, graph, deadline, c, machines):
        task_levels, max_level = calc_levels(graph)
        self.task_levels = task_levels
        self.max_level = max_level
        self.deadline = deadline
        self.c = c
        self.machines = machines

    def calculate_boundaries(self, task, schedule):
        lcb, is_max_predecessor_scheduled = calculate_constant_left_boundary(task, schedule, self.machines)
        rcb, is_min_successor_scheduled = calculate_constant_right_boundary(task, schedule, self.machines, self.deadline)

        lvb, rvb = self._calc_volatile_boundary(task, lcb, rcb)

        lb = lcb + lvb
        rb = rcb + rvb

        # If it is the first time stamp (lcb == 0) or the task is limited by a scheduled predecessor, then there is no
        # need of lvb > 0.
        if lcb == 0 or is_max_predecessor_scheduled:
            lvb = 0

        # If it is the last time stamp (lrb == 0) or the task is limited by a scheduled successor, then there is no
        # need of rvb > 0.
        if rcb == 0 or is_min_successor_scheduled:
            rvb = 0

        # If there is no enough time or cores to execute the task, then the variable boundaries are set to zero.
        if self.deadline - lb - rb < task.runtime:  # or not self.machine.can_schedule_task_in(task, lb, rb):
            lvb = 0
            rvb = 0

        return lcb, lvb, rcb, rvb

    def _calc_volatile_boundary(self, task, lcb, rcb):
        available_time = abs(self.deadline - lcb - rcb)
        available_time_to_use = round((1-self.c) * available_time)
        time_to_variable_boundary = abs(available_time_to_use - available_time)

        task_level = float(self.task_levels[task.id])
        left_c = task_level / (self.max_level + 1)  # Level 0 is the first level

        lvb = round(time_to_variable_boundary * left_c)
        rvb = time_to_variable_boundary - lvb

        return lvb, rvb



