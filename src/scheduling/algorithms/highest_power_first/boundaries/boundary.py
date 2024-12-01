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
        """
            This function calculates the scheduling boundaries for a task. These boundaries define the permissible time
            intervals within which a task can be scheduled while respecting both precedence constraints and resource
            efficiency.

            The Left Constant Boundary (LCB) and Right Constant Boundary (RCB) define the fixed interval limits that
            enforce precedence constraints between tasks. That is, if the task were scheduled inside a Constant
            Boundary, the scheduling would be wrong dua a precedence constraint violation.

            Left Variable Boundary (LVB) and Right Variable Boundary (RVB) aim to optimize the sharing of renewable
            resources among tasks. They ensure tasks are not scheduled too early or too late, such that
            a predecessor or a successor is prevented to be scheduled in an efficient time.

            We have available_time = |deadline - LCB - RCB| and LVB + RVB = round(c * available_time)

            The length of LVB and RVB depends on the task level. As small the task level, smaller the LVB and greater
            the RCB.

        :param task: The task to be scheduled
        :param scheduling: A mapping with the start time of tasks already scheduled
        :return: Left Constant Boundary (LCB),
                Left Variable Boundary (LVB),
                Right Constant Boundary (RCB)
                Right Variable Boundary (RVB)
        """

        lcb, is_limited_by_scheduled_predecessor = calculate_left_boundary(task, scheduling)
        rcb, is_limited_by_scheduled_successor = calculate_right_boundary(task, scheduling, self.deadline)

        available_time = abs(self.deadline - lcb - rcb)
        available_time_to_use = round((1-self.c) * available_time)
        time_to_variable_boundary = abs(available_time_to_use - available_time)

        task_level = float(self.task_levels[task.id])
        left_c = task_level / self.max_level

        lvb = round(time_to_variable_boundary * left_c)
        rvb = time_to_variable_boundary - lvb

        # If it is the first time stamp (lcb == 0) or the task is limited by a scheduled predecessor, then there is no
        # need of lvb > 0.
        if lcb == 0 or is_limited_by_scheduled_predecessor:
            lvb = 0

        # If it is the last time stamp (lrb == 0) or the task is limited by a scheduled successor, then there is no
        # need of rvb > 0.
        if rcb == 0 or is_limited_by_scheduled_successor:
            rvb = 0

        # If there is no enough time to execute the task, then the variable boundaries are set to zero.
        if self.deadline - lcb - lvb - rcb - rvb < task.runtime:
            lvb = 0
            rvb = 0

        return lcb, lvb, rcb, rvb
