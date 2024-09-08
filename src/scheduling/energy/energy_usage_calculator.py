def _append_task_power_events(power_events, task, start_time):
    power_events.append(
        (start_time, 'task', task.power)
    )
    finish_time = start_time + task.runtime
    power_events.append(
        (finish_time, 'task', -task.power)
    )
    power_events.sort(key=lambda d: d[0])  # TODO use binary search tree


def _remove_task_power_event(power_events, task, start_time):
    indexes_to_remove = []
    finish_time = start_time + task.runtime
    for index, data in enumerate(power_events):
        time, type, power = data
        if type == 'task':
            if time == start_time and power == task.power and len(indexes_to_remove) == 0:
                indexes_to_remove.append(index)
            elif time == finish_time and power == -task.power and len(indexes_to_remove) == 1:
                indexes_to_remove.append(index - 1)  # Removing the previous index decreases the current index
                break

    for index_to_remove in indexes_to_remove:
        del power_events[index_to_remove]


def _calculate(power_events):
    # Power along iterations
    green_power = 0
    requested_power = 0

    # Energy used
    brown_energy_used = 0
    green_energy_not_used = 0
    total_energy = 0

    start_time = 0
    for power_event in power_events:

        time, event_type, power = power_event

        # Convert power and duration to energy
        event_duration = time - start_time
        green_energy = event_duration * green_power
        requested_energy = event_duration * requested_power

        if requested_energy > green_energy:
            brown_energy_used += requested_energy - green_energy

        elif requested_energy < green_energy:
            green_energy_not_used += green_energy - requested_energy

        total_energy += requested_energy

        if event_type == 'green_power':
            green_power = power
        elif event_type == 'task':
            requested_power += power
        else:
            raise EventTypeException(f'No eventy type {event_type} defined')

        start_time = time

    return brown_energy_used, green_energy_not_used, total_energy


class EnergyUsageCalculator:

    def __init__(self, graph, green_energy, interval_size):
        self.graph = graph
        self.green_energy = green_energy
        self.interval_size = interval_size
        self.power_events = []
        self._init()

    def _append_green_power_events(self, power_events):
        g_power_start_time = 0
        for g_power in self.green_energy:
            power_events.append(
                (g_power_start_time, 'green_power', g_power)
            )
            g_power_start_time += self.interval_size
        power_events.append(
            (g_power_start_time, 'green_power', 0)
        )

    def _init(self):
        self.power_events = []
        self._append_green_power_events(self.power_events)

    def reset(self):
        self._init()

    def add_scheduled_task(self, new_task, start_time):
        _append_task_power_events(self.power_events, new_task, start_time)

    def remove_scheduled_task(self, scheduled_task, start_time):
        _remove_task_power_event(self.power_events, scheduled_task, start_time)

    def calculate_energy_usage(self):
        return _calculate(self.power_events)

    def calculate_energy_usage_for_scheduling(self, scheduling):

        self._init()

        # Append scheduling power events
        for task_id, start_time in scheduling.items():
            scheduled_task = self.graph.get_task(task_id)
            _append_task_power_events(self.power_events, scheduled_task, start_time)

        # Sort power events by time
        self.power_events.sort(key=lambda d: d[0])

        return _calculate(self.power_events)

    def get_green_power_available(self):
        # TODO test this function
        actual_green_power_available = []
        current_green_power = 0
        current_power_request = 0
        previous_time = 0

        last_power_added = -1

        def add_actual_green_power_available(time, actual_green_power_available, current_green_power,
                                             current_power_request, last_power_added):
            available_green_power = current_green_power - current_power_request
            if available_green_power < 0:
                available_green_power = 0

            if available_green_power != last_power_added:
                # TODO fix overlapped time
                actual_green_power_available.append(
                    (time, available_green_power)
                )
                return available_green_power
            return last_power_added

        for time, type, power in self.power_events:
            is_the_same_time = (time == previous_time)

            if not is_the_same_time:
                # Add previous time a power availability
                last_power_added = add_actual_green_power_available(previous_time, actual_green_power_available,
                                                                    current_green_power,
                                                                    current_power_request, last_power_added)

            if type == 'green_power':
                current_green_power = power
            elif type == 'task':
                current_power_request += power
            else:
                raise EventTypeException(f'No eventy type {type} defined')
            previous_time = time

        add_actual_green_power_available(previous_time, actual_green_power_available, current_green_power,
                                         current_power_request, last_power_added)
        return actual_green_power_available


class EventTypeException(Exception):
    def __init__(self, message):
        super().__init__(message)
