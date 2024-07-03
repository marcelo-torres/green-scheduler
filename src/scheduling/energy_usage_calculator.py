def _append_task_power_events(power_events, task, start_time):
    power_events.append(
        (start_time, 'task', task.power)
    )
    finish_time = start_time + task.runtime
    power_events.append(
        (finish_time, 'task', -task.power)
    )


def _remove_task_power_event(power_events, task, start_time):
    indexes_to_remove = []
    finish_time = start_time + task.runtime
    for index, data in enumerate(power_events):
        time, type, power = data
        if type == 'task':
            if time == start_time and power == task.power and len(indexes_to_remove) == 0:
                indexes_to_remove.append(index)
            elif time == finish_time and power == -task.power and len(indexes_to_remove) == 1:
                indexes_to_remove.append(index-1) # Removing the previous index decreases the current index
                break
    for index_to_remove in indexes_to_remove:
        del power_events[index_to_remove]


def _append_new_task(power_events, new_task, new_task_start_time):
    if new_task is None or new_task_start_time is None:
        #print('new_task and new_task_start_time cannot be None. Current values: {new_task} and {new_task_start_time}')
        return

    _append_task_power_events(power_events, new_task, new_task_start_time)


class EnergyUsageCalculator:

    def __init__(self, graph, green_energy, interval_size):
        self.graph = graph
        self.green_energy = green_energy
        self.interval_size = interval_size

        self.power_events = []

    def _append_green_power_events(self, power_events):
        g_power_start_time = 0
        for g_power in self.green_energy:
            power_events.append(
                (g_power_start_time, 'green_power', g_power)
            )
            g_power_start_time += self.interval_size

    def _append_scheduling_power_events(self, power_events, scheduling):
        for task_id, start_time in scheduling.items():
            scheduled_task = self.graph.get_task(task_id)
            _append_task_power_events(power_events, scheduled_task, start_time)


    def _calculate(self, power_events):
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
                raise Exception(f'No eventy type {event_type} defined')

            start_time = time

        return brown_energy_used, green_energy_not_used, total_energy

    def init(self):
        self.power_events = []
        self._append_green_power_events(self.power_events)

    def reset(self):
        self.init()

    def add_scheduled_task(self, new_task, start_time):
        _append_task_power_events(self.power_events, new_task, start_time)

    def remove_scheduled_task(self, scheduled_task, start_time):
        _remove_task_power_event(self.power_events, scheduled_task, start_time)

    def calculate_energy_usage(self):
        return self._calculate(self.power_events)

    def calculate_energy_usage_for_scheduling(self, scheduling, new_task=None, new_task_start_time=None):

        power_events = []

        self._append_green_power_events(power_events)
        self._append_scheduling_power_events(power_events, scheduling)
        _append_new_task(power_events, new_task, new_task_start_time)

        # Sort power events by time
        power_events.sort(key=lambda d: d[0])

        return self._calculate(power_events)

    def get_green_power_available(self):
        # TODO test this function
        actual_green_power_available = []
        current_green_power = 0
        current_power_request = 0
        previous_time = None

        def add_actual_green_power_available(actual_green_power_available, current_green_power, current_power_request):
            available_green_power = current_green_power - current_power_request
            if available_green_power < 0:
                available_green_power = 0

            # TODO fix overlapped time
            actual_green_power_available.append(
                (time, available_green_power)
            )

        for time, type, power in self.power_events:
            if time is not previous_time and time is not None and previous_time is not None:
                add_actual_green_power_available(actual_green_power_available, current_green_power,
                                                 current_power_request)
            if type == 'green_power':
                current_green_power = power
            elif type == 'task':
                current_power_request += power
            previous_time = time

        add_actual_green_power_available(actual_green_power_available, current_green_power, current_power_request)
        return actual_green_power_available
