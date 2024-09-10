import pyredblack

TASK_EVENT = 'task'
G_POWER_EVENT = 'green_power'

class PowerEvents:

    def __init__(self):
        self.power_events = pyredblack.rbdict()
        self.task_scheduling = {}

    def _add_power_event(self, event_type, power, time):
        if time not in self.power_events:
            events = []
            self.power_events[time] = events
        else:
            events = self.power_events[time]

        events.append(
            (event_type, power)
        )

    def _remove_where(self, event_type, power, time):
        events = self.power_events[time]
        for index, data in enumerate(events):
            current_event_type, current_power = data
            if event_type == current_event_type and power == current_power:
                del events[index]
                if len(events) == 0:
                    del self.power_events[time]
                break

    def append_task_power(self, task, start_time):
        self._add_power_event(TASK_EVENT, task.power, start_time)
        finish_time = start_time + task.runtime
        self._add_power_event(TASK_EVENT, -task.power, finish_time)
        self.task_scheduling[task.id] = start_time

    def append_green_power(self, green_power, time):
        self._add_power_event(G_POWER_EVENT, green_power, time)

    def remove_task_power_event(self, task):
        start_time = self.task_scheduling[task.id]
        finish_time = start_time + task.runtime

        self._remove_where(TASK_EVENT, task.power, start_time)
        self._remove_where(TASK_EVENT, -task.power, finish_time)

        del self.task_scheduling[task.id]

    def items(self):
        """"
        :return: a tuple in the form:
            (
                time,
                [(event_type, power), (event_type, power)]
            )
        """
        return self.power_events.items()


def _calculate(power_events):
    # Power along iterations
    green_power = 0
    requested_power = 0

    # Energy used
    brown_energy_used = 0
    green_energy_not_used = 0
    total_energy = 0

    start_time = 0
    for time, events in power_events.items():

        # Convert power and duration to energy
        event_duration = time - start_time
        green_energy = event_duration * green_power
        requested_energy = event_duration * requested_power

        if requested_energy > green_energy:
            brown_energy_used += requested_energy - green_energy

        elif requested_energy < green_energy:
            green_energy_not_used += green_energy - requested_energy

        total_energy += requested_energy

        for event_type, power in events:
            if event_type == G_POWER_EVENT:
                green_power = power
            elif event_type == TASK_EVENT:
                requested_power += power
            else:
                raise EventTypeException(f'No eventy type {event_type} defined')

        start_time = time

    return brown_energy_used, green_energy_not_used, total_energy


class EnergyUsageCalculator:

    def __init__(self, green_energy, interval_size):
        self.green_energy = green_energy
        self.interval_size = interval_size
        self._init()

    def _append_green_power_events(self):
        g_power_start_time = 0
        for g_power in self.green_energy:
            self.power_events.append_green_power(g_power, g_power_start_time)
            g_power_start_time += self.interval_size
        self.power_events.append_green_power(0, g_power_start_time)

    def _init(self):
        self.power_events = PowerEvents()
        self._append_green_power_events()

    def reset(self):
        self._init()

    def add_scheduled_task(self, new_task, start_time):
        self.power_events.append_task_power(new_task, start_time)

    def remove_scheduled_task(self, scheduled_task):
        self.power_events.remove_task_power_event(scheduled_task)

    def calculate_energy_usage(self):
        return _calculate(self.power_events)

    def calculate_energy_usage_for_scheduling(self, scheduling, graph):

        self._init()

        # Append scheduling power events
        for task_id, start_time in scheduling.items():
            scheduled_task = graph.get_task(task_id)
            self.power_events.append_task_power(scheduled_task, start_time)

        return _calculate(self.power_events)

    def get_green_power_available(self):
        # TODO test this function
        actual_green_power_available = []
        current_green_power = 0
        current_power_request = 0

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

        for time, events in self.power_events.items():

            for event_type, power in events:
                if event_type == G_POWER_EVENT:
                    current_green_power = power
                elif event_type == TASK_EVENT:
                    current_power_request += power
                else:
                    raise EventTypeException(f'No eventy type {event_type} defined')

            last_power_added = add_actual_green_power_available(time, actual_green_power_available,
                                                                current_green_power,
                                                                current_power_request, last_power_added)

        return actual_green_power_available


class EventTypeException(Exception):
    def __init__(self, message):
        super().__init__(message)
