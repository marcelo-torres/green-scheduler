from collections import deque

from src.scheduling.task_graph.task import Task


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
        # TODO tests if it is possible to request more than one next interval
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
        if previous_time == -1 or time <= task_start:
            previous_time = time
            previous_green_power = green_power
            continue

        if previous_time < task_start < time:
            interval_length = time - task_start
        else:
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



if __name__ == '__main__':
    # TODO move to tests class
    _test_slice_green_power_available_list()
    _test_find_min_brown_energy()
    _test_calculate_brown_energy_of_task()
