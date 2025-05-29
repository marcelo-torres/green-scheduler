from src.scheduling.algorithms.lpt.longest_processing_time_first import lpt
from src.scheduling.model.cluster import Cluster
from src.scheduling.util.lpt_topological_sort import LtpTopologicalSort
#
#
# class LptBoundaryShiftEstimator:
#
#     def __init__(self, machines, graph):
#         self.machines = machines
#         self.graph = graph
#         self.reference_schedule = self.estimate(graph, machines)
#         self.topological_sort_cached =  LtpTopologicalSort(graph)
#
#
#     def estimate(self, graph, machines):
#         clusters = Cluster('temp', None, machines)
#         return lpt(graph, clusters)
#
#     def calculate_boundaries(self, task, current_schedule, deadline):
#         temp_schedule = self.update_reference_schedule(current_schedule)
#         del temp_schedule[task.id]
#
#         for t_rank, t in self.topological_sort_cached.lpt_topological_list_until(task):
#             pass
#
#         for t_rank, t in self.topological_sort_cached.lpt_topological_inverse_list_until(task):
#             pass
#
#     def update_reference_schedule(self, current_schedule):
#         temp_schedule = self.reference_schedule.copy()
#
#         for key, item in current_schedule.items():
#             temp_schedule[key] = item
#
#         return temp_schedule
#
# def _schedule_in_min_brown_energy(task, machines, schedule, deadline, energy_usage_calculator, max_start_mode):
#     best_machine = None
#     selected_start_time = None
#     smallest_brown_energy = float('inf')
#
#     interval_available = (deadline - rb) - lb
#     if interval_available < task.runtime:
#         raise Exception(
#             f'Not enough time to schedule task {task.id}! Task runtime: {task.runtime}; Interval available: {interval_available}')
#
#     for machine in machines:
#         for start, end in machine.search_intervals_to_schedule_task(task, lb, deadline - rb):
#
#             start_time, brown_energy = find_min_brown_energy_start(task, start, end, green_power_available,
#                                                                    max_start_mode=max_start_mode)
#
#             if _is_better(brown_energy, smallest_brown_energy, start_time, selected_start_time, max_start_mode):
#                 best_machine = machine
#                 selected_start_time = start_time
#                 smallest_brown_energy = brown_energy
#
#     if best_machine is None:
#         raise Exception(f'No machine available to schedule task {task.id}!')
#
#     schedule[task.id] = selected_start_time, best_machine.id
#     best_machine.schedule_task(task, selected_start_time)
#     energy_usage_calculator.add_scheduled_task(task, selected_start_time)
#
# def _is_better(brown_energy, smallest_brown_energy, start_time, selected_start_time, max_start_mode):
#     if brown_energy < smallest_brown_energy:
#         return True
#
#     if brown_energy == smallest_brown_energy:
#         if max_start_mode:
#             return start_time > selected_start_time
#         else:
#             return start_time < selected_start_time
#
#     return False
