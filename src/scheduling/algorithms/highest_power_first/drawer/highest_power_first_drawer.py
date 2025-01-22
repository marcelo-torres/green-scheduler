from src.scheduling.drawer.drawer import Drawer


def draw_rectangles(drawer, events):
    """
    It used a Drawer to draw rectangles to represent total power usage. A power event represents energy usage changes.

    :param drawer: Drawer object
    :param events: a list of power events
    :return: max energy usage
    """

    max_power = -1
    active_tasks = []
    last_time = -1
    for task, time, event_type in events:
        if last_time >= 0 and time is not last_time:
            duration = time - last_time
            current_power = 0

            for active_task in active_tasks:
                drawer.add_rectangle(duration, active_task.power, last_time, current_power, description=active_task.id)
                current_power += active_task.power

            if current_power > max_power:
                max_power = current_power

        if event_type == 'start':
            active_tasks.append(task)
        else:
            active_tasks.remove(task)
        last_time = time

    return max_power


def draw_line(drawer, events, color='blue'):
    """
    It used a Drawer to represent the power usage as line chart. A power event represents energy usage changes.

    :param drawer: Drawer object
    :param events: a list of power events
    :return: max energy usage
    """

    max_power = -1
    active_tasks = []
    time_list = []
    power_list = []
    last_time = -1
    for task, time, event_type in events:
        if last_time >= 0 and time is not last_time:
            duration = time - last_time
            current_power = 0
            for active_task in active_tasks:
                current_power += active_task.power
            time_list.append(last_time)
            time_list.append(last_time + duration)
            power_list.append(current_power)
            power_list.append(current_power)

            if current_power > max_power:
                max_power = current_power

        if event_type == 'start':
            active_tasks.append(task)
        else:
            active_tasks.remove(task)
        last_time = time

    # Add first point (to close the line)
    time_list.insert(0, time_list[0])  # duplicate the first time
    power_list.insert(0, 0)  # set power to 0 (y-axis)

    # Add last point (to close the line)
    time_list.append(time_list[-1])  # duplicate the last time (x-axis)
    power_list.append(0)  # set power to 0 (y-axis)

    drawer.add_line(time_list, power_list, color=color)
    return max_power


def draw_scheduling(lcb, lvb, rcb, rvb, deadline, green_energy, interval_size, scheduling, graph, max_power=None):
    drawer = Drawer(max_power, deadline)
    drawer.add_constant_boundary(0, lcb)
    drawer.add_variable_boundary(lcb, lvb)
    drawer.add_variable_boundary(deadline - rvb - rcb, rvb)
    drawer.add_constant_boundary(deadline - rcb, rcb)

    interval_start = 0
    for power in green_energy:
        drawer.add_green_energy_availability(interval_start, interval_size, power)
        interval_start += interval_size

    events = []
    for task_id, start_time in scheduling.items():
        task = graph.get_task(task_id)
        events.append(
            (task, start_time, 'start')
        )

        end_time = start_time + task.runtime
        events.append(
            (task, end_time, 'end')
        )

    # Sort by time and event type (start comes first)
    events.sort(key=lambda e: (e[1], 0 if e[2] == "start" else 1))

    is_a_lot_of_events = len(events) > 50
    if is_a_lot_of_events:
        current_max_power = draw_line(drawer, events)
    else:
        current_max_power = draw_rectangles(drawer, events)

    if max_power is None:
        drawer.height = 1.5 * current_max_power
    return drawer
