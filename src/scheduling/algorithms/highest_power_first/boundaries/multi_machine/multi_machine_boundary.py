from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.lpt_boundary_estimator import \
    LptBoundaryEstimator
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_constant_left_boundary import \
    calculate_constant_left_boundary
from src.scheduling.algorithms.highest_power_first.boundaries.multi_machine.multi_machine_constant_right_boundary import \
    calculate_constant_right_boundary
from src.scheduling.util.calc_levels import calc_levels


class MultiMachineBoundaryCalculator:

    def __init__(self, graph, deadline, c, machines, strategy='default'):
        self.graph = graph
        task_levels, max_level = calc_levels(graph)
        self.task_levels = task_levels
        self.tasks_by_level = _split_tasks_by_level(task_levels)
        self.max_level = max_level
        self.deadline = deadline
        self.c = c
        self.machines = machines

        self.schedule_left_debug = {}

        if strategy == 'lpt-path':
            self.calc_lcb = lambda task, schedule: calculate_constant_left_boundary(task, schedule, self.machines, use_lpt=True, same_level_tasks=self._get_tasks_in_the_same_level(task))
            self.calc_rcb = lambda task, schedule: calculate_constant_right_boundary(task, schedule, self.machines, self.deadline, use_lpt=True)

        elif strategy == 'lpt-full':
            self.lpt_boundary_estimator = LptBoundaryEstimator(machines, graph)

            self.calc_lcb = lambda task, schedule: self.lpt_boundary_estimator.calculate_constant_left_boundary(task, schedule)
            self.calc_rcb = lambda task, schedule: self.lpt_boundary_estimator.calculate_constant_right_boundary(task, schedule, self.deadline)

        else:
            self.calc_lcb = lambda task, schedule: calculate_constant_left_boundary(task, schedule, self.machines, use_lpt=False, same_level_tasks=None)
            self.calc_rcb = lambda task, schedule: calculate_constant_right_boundary(task, schedule, self.machines, self.deadline, use_lpt=False)

    def calculate_boundaries(self, task, schedule):

        lcb, is_max_predecessor_scheduled = self.calc_lcb(task, schedule)
        rcb, is_min_successor_scheduled = self.calc_rcb(task, schedule)

        lvb, rvb = self._calc_volatile_boundary(task, lcb, rcb)

        # If it is the first time stamp (lcb == 0) or the task is limited by a scheduled predecessor, then there is no
        # need of lvb > 0.
        if lcb == 0 or is_max_predecessor_scheduled:
            lvb = 0

        # If it is the last time stamp (lrb == 0) or the task is limited by a scheduled successor, then there is no
        # need of rvb > 0.
        if rcb == 0 or is_min_successor_scheduled:
            rvb = 0

        lb = lcb + lvb
        rb = rcb + rvb

        # If there is no enough time or cores to execute the task, then the variable boundaries are set to zero.
        if self.deadline - lb - rb < task.runtime:  # or not self.machine.can_schedule_task_in(task, lb, rb):
            lvb = 0
            rvb = 0
            lb = lcb
            rb = rcb

        assert task.runtime <= self.deadline - (lb + rb), f'{task.runtime} <= {self.deadline} - ({lb} + {rb}) = {self.deadline - (lb + rb)}'


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

    def _get_tasks_in_the_same_level(self, task):
        level = self.task_levels[task.id]
        return [self.graph.get_task(t_id) for t_id in self.tasks_by_level[level]]


def _split_tasks_by_level(task_levels):

    tasks_by_level = {}

    for task_id, level in task_levels.items():
        if level not in tasks_by_level:
            tasks_by_level[level] = [task_id]
        else:
            tasks_by_level[level].append(task_id)

    return tasks_by_level
