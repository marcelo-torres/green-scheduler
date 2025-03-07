class IntervalException(Exception):
    def __init__(self, task, start, end):
        super().__init__(f'Interval length is not enough to schedule task {task.id}: runtime={task.runtime} start={start} end={end}')


def find_min_brown_energy_start(task, start, end, green_energy_available, max_start_mode=False):
    if task.power == 0:
        return start

    if end - start < task.runtime:
        raise IntervalException(task, start, end)

    green_power_interval = _slice_green_power_available_list(green_energy_available, start, end)
    start_min, task_min_brown_energy_usage = _find_min_brown_energy_in_interval(task, green_power_interval, max_start_mode=max_start_mode)

    return start + start_min, task_min_brown_energy_usage


def find_min_brown_energy(task, lb, rb, deadline, green_energy_available, max_start_mode=False):  # TODO remove
    return find_min_brown_energy_start(task, lb, deadline - rb, green_energy_available, max_start_mode=max_start_mode)


def _slice_green_power_available_list(actual_green_power_available, start, end):
    available_green_powers = []

    g_power_iter = iter(actual_green_power_available)
    previous_time, previous_available_green_power = next(g_power_iter, (float('inf'), -1))

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


def _find_min_brown_energy_in_interval(task, green_power_interval, max_start_mode=False):
    if len(green_power_interval) == 0:
        return 0, task.runtime

    start_times_to_verify = _start_times_to_verify(task, green_power_interval)

    start_min = -1
    min_brown_energy_usage = float('inf')

    for start_time, brown_energy_usage in _brown_energy_in_interval(task, start_times_to_verify, green_power_interval):

        # Verify if it is minimal
        if (brown_energy_usage < min_brown_energy_usage
                or (max_start_mode and brown_energy_usage == min_brown_energy_usage)):
            min_brown_energy_usage = brown_energy_usage
            start_min = start_time

    return start_min, min_brown_energy_usage


def _brown_energy_in_interval(task, start_times_to_verify, green_power_interval):
    g_iter = iter(green_power_interval)
    current_green_events = _load_current_power_green_events(g_iter, task)

    # Calculate the first time
    first_start_time = start_times_to_verify.pop(0)
    current_brown_energy_usage = _calculate_brown_energy_of_task(task, first_start_time, current_green_events)

    yield first_start_time, current_brown_energy_usage

    previous_start_time = first_start_time
    for start_time in start_times_to_verify:

        # Calculate brown energy change
        b_e_to_increase = _brown_energy_to_increase(start_time, previous_start_time, current_green_events, g_iter, task)
        b_e_to_decrease = _brown_energy_to_decrease(start_time, previous_start_time, current_green_events, task)

        # Adjust brown energy usage
        current_brown_energy_usage += b_e_to_increase
        current_brown_energy_usage -= b_e_to_decrease
        current_brown_energy_usage = round_internal(current_brown_energy_usage)

        previous_start_time = start_time

        yield start_time, current_brown_energy_usage


def _load_current_power_green_events(g_iter, task):
    current_green_events = []

    time = -1
    while time < task.runtime:
        time, power = next(g_iter, (-1, 0))
        current_green_events.append(
            (time, power)
        )

    return current_green_events


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

        brown_power = (task.power - previous_green_power) if task.power > previous_green_power else 0
        partial_brown_energy = calculate_energy(execution_time_in_interval, brown_power)
        current_brown_energy_usage += partial_brown_energy

        previous_time = time
        previous_green_power = green_power

        if already_computed_time == task.runtime:
            break

    return round_internal(current_brown_energy_usage)


def round_internal(value):
    return round(value, 4)


def calculate_energy(power, time):
    result = round_internal(power) * round_internal(time)
    return round_internal(result)


def _brown_energy_to_increase(start_time, previous_start_time, current_green_events, g_iter, task):
    current_brown_energy_usage = 0

    finish_time = start_time + task.runtime
    last_finish_time = previous_start_time + task.runtime

    time_to_compute = start_time - previous_start_time
    already_computed_time = 0

    start = last_finish_time
    while already_computed_time < time_to_compute:
        interval_end = current_green_events[-1][0]
        if finish_time <= interval_end:
            duration = finish_time - start
            start = finish_time
        else:
            duration = interval_end - start
            start = interval_end

            # Next interval
            new_last_time, new_g_power = next(g_iter)
            current_green_events.append(
                (new_last_time, new_g_power)
            )

        last_interval_g_power = current_green_events[-2][1]
        brown_power_to_increase = task.power - last_interval_g_power if task.power > last_interval_g_power else 0

        brown_energy_to_increase = calculate_energy(brown_power_to_increase, duration)
        current_brown_energy_usage += brown_energy_to_increase

        already_computed_time += duration

    return round_internal(current_brown_energy_usage)


def _brown_energy_to_decrease(start_time, previous_start_time, current_green_events, task):
    _, g_power = current_green_events[0]

    brown_power_to_decrease = task.power - g_power if task.power > g_power else 0
    duration = start_time - previous_start_time

    brown_energy_to_decrease = calculate_energy(brown_power_to_decrease, duration)

    # pop the first interval if start time is in the second interval
    if len(current_green_events) > 1 and start_time >= current_green_events[1][0]:
        current_green_events.pop(0)

    return brown_energy_to_decrease


def _start_times_to_verify(task, green_power_interval):
    """
    This functions list all the start times to verify which one provides minimal brown energy usage. This approach tests
    only the necessary time stamps.

    For the green power interval [(0, 5), (5, 8), (7, 3), (10, 0)] we have:
        - 5 seconds of 5W (5s - 0s)
        - 2 seconds of 8W (7s - 5s)
        - 3 seconds of 3W (10s - 7s)

    As in our model a task have a constant power request, we do not need to recalculate the power usage during the same
    power interval. Then, we need to calculate the timestamps related to task start and task end.

    :param task: task to be scheduled
    :param green_power_interval: list of tuples in the form (start time, green power available)
    :return: a list of start times to verify which one provide minimal green energy usage
    """

    if len(green_power_interval) == 0:
        return []

    start_time = green_power_interval[0][0]
    end_time = green_power_interval[-1][0]

    task_can_start = []
    task_can_finish = []

    for time, _ in green_power_interval:

        # Check if a task can start at a given time
        if time + task.runtime <= end_time:
            task_can_start.append(time)

        # it is commented because it causes a bug
        # Check if a task can finish at a given time
        #previous_interval_length = time - previous_time
        #if previous_interval_length < task.runtime:
            # If the previous interval length is bigger than task runtime, then the task do not span across several
            # intervals. Therefore, there is no need to schedule a task to finish by the end of the interval, because
            # the brown power usage would be the same then by scheduling at beginning of the interval.

        # Check if there is enough time to task end by time
        time_to_start = time - task.runtime
        if time_to_start > start_time:
            task_can_finish.append(time_to_start)

    return _merge_sorted_lists_without_repeated_elements(task_can_start, task_can_finish)


def _merge_sorted_lists_without_repeated_elements(list1, list2):
    list1_iter = iter(list1)
    list2_iter = iter(list2)

    merge_list = []

    l1_element = next(list1_iter, None)
    l2_element = next(list2_iter, None)

    while l1_element is not None and l2_element is not None:
        if l1_element < l2_element:
            merge_list.append(l1_element)
            l1_element = next(list1_iter, None)

        elif l2_element < l1_element:
            merge_list.append(l2_element)
            l2_element = next(list2_iter, None)

        else:
            # If the elements are equal, add just one
            merge_list.append(l1_element)
            l1_element = next(list1_iter, None)
            l2_element = next(list2_iter, None)

    while l1_element is not None:
        merge_list.append(l1_element)
        l1_element = next(list1_iter, None)

    while l2_element is not None:
        merge_list.append(l2_element)
        l2_element = next(list2_iter, None)

    return merge_list



