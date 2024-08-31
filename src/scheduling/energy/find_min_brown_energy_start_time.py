from collections import deque

from src.scheduling.task_graph.task import Task


def find_min_brown_energy_greedy(task, lb, rb, deadline, calculator):

    start = lb

    start_min = start
    min_brown_energy_usage = float('inf')

    while start + task.runtime <= deadline - rb:

        calculator.add_scheduled_task(task, start)
        brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage()
        calculator.remove_scheduled_task(task, start)

        if brown_energy_used < min_brown_energy_usage:
            min_brown_energy_usage = brown_energy_used
            start_min = start
        start += 1
    return start_min


def find_min_brown_energy(task, lb, rb, deadline, green_energy_available):

    if task.power == 0:
        return lb

    start = lb
    end = deadline-rb

    if end - start < task.runtime:
        raise Exception(f'Interval length is not enough to schedule task {task.id}: runtime={task.runtime} start={start} end={end}')

    green_power_interval = _slice_green_power_available_list(green_energy_available, start, end)
    start_min = start + _find_min_brown_energy_in_interval(task, green_power_interval)

    return start_min


def _slice_green_power_available_list(actual_green_power_available, start, end):
    available_green_powers = []

    g_power_iter = iter(actual_green_power_available)

    previous_time, previous_available_green_power = next(g_power_iter, (float('inf'), -1))
    current_time = -1

    while previous_time <= end:
        current_time, current_available_green_power = next(g_power_iter, (float('inf'), -1))

        if start <= previous_time <= end:
            available_green_powers.append(
                (previous_time-start, previous_available_green_power)
            )
        elif previous_time < start < current_time:
            available_green_powers.append(
                (0, previous_available_green_power)
            )
        if previous_time < end < current_time:
            available_green_powers.append(
                (end-start, previous_available_green_power)
            )

        previous_time = current_time
        previous_available_green_power = current_available_green_power

    return available_green_powers


def _find_min_brown_energy_in_interval(task, green_power_interval):

    if task.runtime == 0:
        return 0

    start_min = 0
    min_brown_energy_usage = float('inf')

    task_start = 0
    task_end = task_start + task.runtime

    if len(green_power_interval) == 0:
        #print('warn: len(green_power_interval) == 0')
        return 0

    # Load current green events
    current_green_events = deque()
    g_power_iter = iter(green_power_interval)
    time = -1
    while time < task_end:
        time, power = next(g_power_iter, (-1, 0))
        current_green_events.append(
            (time, power)
        )

    next_power_event = None

    while True:
        # Compute brown energy usage
        current_brown_energy_usage = _calculate_brown_energy_of_task(task, task_start, current_green_events)
        if current_brown_energy_usage < min_brown_energy_usage:
            min_brown_energy_usage = current_brown_energy_usage
            start_min = task_start

        # Calculate distances
        second_current_power_event = current_green_events[1] if len(current_green_events) >= 2 else None
        if next_power_event is None:
            next_power_event = next(g_power_iter, None)

        distance_to_second = float('inf')
        if second_current_power_event:
            time, green_power = second_current_power_event
            distance_to_second = time - task_start

        distance_to_next = float('inf')
        if next_power_event:
            time, green_power = next_power_event
            distance_to_next = time - task_end

        # Choose next start
        if distance_to_second <= distance_to_next:
            task_start += distance_to_second
            if len(current_green_events) > 0:
                first_event_time, p = current_green_events[0]
                last_event_time, p = green_power_interval[-1]
                if last_event_time - first_event_time >= task.runtime:
                    current_green_events.popleft()
                else:
                    break

        elif distance_to_next < distance_to_second:
            task_start += distance_to_next
            current_green_events.append(next_power_event)
            next_power_event = None

        # Adjust task end taking into account the new task start
        task_end = task_start + task.runtime

        # If current interval length is lesser than task runtime, then increase the interval
        # TODO test if it is possible to request more than one next interval
        first_event_time, p = current_green_events[0]
        last_event_time, p = current_green_events[-1]
        if last_event_time - first_event_time < task.runtime:
            if next_power_event is None:
                next_power_event = next(g_power_iter, None)
                if next_power_event is None:
                    break

            current_green_events.append(next_power_event)
            next_power_event = None

    return start_min


def _calculate_brown_energy_of_task(task, task_start, current_green_events):
    current_brown_energy_usage = 0

    previous_time = -1
    previous_green_power = -1

    already_computed_time = 0

    for time, green_power in current_green_events:
        if previous_time == -1:
            previous_time = time
            previous_green_power = green_power
            continue

        interval_length = time - previous_time

        remaining_task_runtime = task.runtime - already_computed_time
        if remaining_task_runtime < interval_length:
            execution_time_in_interval = remaining_task_runtime
        else:
            execution_time_in_interval = interval_length

        already_computed_time += execution_time_in_interval

        # execution_time_in_interval = interval_length if interval_length <= task_end else task.runtime
        brown_power = (task.power - previous_green_power) if task.power > previous_green_power else 0
        partial_brown_energy = execution_time_in_interval * brown_power
        current_brown_energy_usage += partial_brown_energy

        previous_time = time
        previous_green_power = green_power

        if already_computed_time == task.runtime:
            break

    return current_brown_energy_usage


# def _find_min_brown_energy_in_interval_v2(task, green_power_interval):
#
#     task_start = 0
#     task_end = task_start + task.runtime
#
#     # Load current green events
#     current_green_events = deque()
#     g_power = deque(green_power_interval)
#     time = -1
#     while time < task_end:
#         time, power = g_power.popleft()
#         current_green_events.append(
#             (time, power)
#         )
#
#     start_min = 0
#     next_power_event = None
#     min_brown_energy_usage = float('inf')
#     while len(deque) > 0:
#
#         # Compute brown energy
#         current_brown_energy_usage = _calculate_brown_energy_of_task(task, task_start, current_green_events)
#
#         if current_brown_energy_usage < min_brown_energy_usage:
#             min_brown_energy_usage = current_brown_energy_usage
#             start_min = task_start
#
#         # Calculate distances
#         second_current_power_event = current_green_events[1] if len(current_green_events) >= 2 else None
#
#         if next_power_event is None and len(g_power) > 0:
#             next_power_event = g_power.popleft()
#
#         distance_to_second = float('inf')
#         if second_current_power_event:
#             time, green_power = second_current_power_event
#             distance_to_second = time - task_start
#
#         distance_to_next = float('inf')
#         if next_power_event:
#             time, green_power = next_power_event
#             distance_to_next = time - task_end
#
#         # Select next start
#         if distance_to_second <= distance_to_next:
#             task_start += distance_to_second
#             if len(current_green_events) > 0:
#                 first_event_time, p = current_green_events[0]
#                 last_event_time, p = g_power[-1]
#                 if last_event_time - first_event_time >= task.runtime:
#                     current_green_events.popleft()
#                 else:
#                     break
#
#         elif distance_to_next < distance_to_second:
#             task_start += distance_to_next
#             current_green_events.append(next_power_event)
#             next_power_event = None
#         pass


def _test_slice_green_power_available_list():
    def validate(start, end, green_power_available, expected):
        sliced = _slice_green_power_available_list(green_power_available, start, end)
        print(sliced)
        assert sliced == expected


    actual_green_power_available = [
        (0, 100),
        (10, 200),
        (15, 0),
        (20, 130)
    ]

    validate(5, 12, actual_green_power_available, [(0, 100), (5, 200), (7, 200)])
    validate(0, 20, actual_green_power_available, [(0, 100), (10, 200), (15, 0), (20, 130)])
    validate(0, 17, actual_green_power_available, [(0, 100), (10, 200), (15, 0), (17, 0)])
    validate(17, 20, actual_green_power_available, [(0, 0), (3, 130)])
def _test_find_min_brown_energy():
    green_energy_available = [
        (0, 100),
        (10, 200),
        (15, 0),
        (20, 130)
    ]

    def validate(expected_start_min, green_energy_available, task_runtime, task_power):
        task = Task(1, task_runtime, task_power)
        start_min = _find_min_brown_energy_in_interval(task, green_energy_available)
        print(start_min)
        assert start_min == expected_start_min

    validate(10, green_energy_available, 2, 200)

    validate(0, green_energy_available, 10, 100)
    validate(0, green_energy_available, 15, 100)
    validate(0, green_energy_available, 20, 100)

    validate(8, green_energy_available, 7, 200)

    validate(10, green_energy_available, 5, 200)

    validate(0, [(0,0), (10, 21)], 0, 0)
    validate(0, [(0, 20), (13, 40), (26, 30), (29, 40)], 15, 10)
def _test_calculate_brown_energy_of_task():

    green_energy_available = [
        (0, 100),
        (10, 200),
        (15, 0),
        (20, 130)
    ]

    def validate(task_runtime, task_power, task_start, current_green_events, expected_brown_energy):
        task = Task(1, task_runtime, task_power)
        brown_energy = _calculate_brown_energy_of_task(task, task_start, current_green_events)
        print(brown_energy)
        assert brown_energy == expected_brown_energy

    validate(10, 100, 0, green_energy_available, 0)
    validate(5, 200, 10, green_energy_available, 0)
    validate(5, 199, 10, green_energy_available, 0)

    validate(15, 200, 0, green_energy_available, 1000)

    validate(20, 100, 0, green_energy_available, 500)

    validate(20, 200, 0, green_energy_available, 2000)

    validate(3, 200, 17, [(15, 0), (20, 130)], 600)


if __name__ == '__main__':
    # TODO move to test class
    _test_slice_green_power_available_list()
    _test_find_min_brown_energy()
    _test_calculate_brown_energy_of_task()
