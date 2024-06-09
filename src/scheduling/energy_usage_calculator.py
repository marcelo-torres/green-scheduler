def _append_task_power_events(power_events, task, start_time):
    power_events.append(
        (start_time, 'task', task.power)
    )
    finish_time = start_time + task.runtime
    power_events.append(
        (finish_time, 'task', -task.power)
    )


def _append_new_task(power_events, new_task, new_task_start_time):
    if new_task is None or new_task_start_time is None:
        raise Exception(f'new_task and new_task_start_time cannot be None. Current values: {new_task} and {new_task_start_time}')

    _append_task_power_events(power_events, new_task, new_task_start_time)


class EnergyUsageCalculator:
    
    def __init__(self, graph, green_energy, interval_size):
        self.graph = graph
        self.green_energy = green_energy
        self.interval_size = interval_size

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

    def calculate_energy_usage(self, scheduling, new_task=None, new_task_start_time=None):

        power_events = []

        self._append_green_power_events(power_events)
        self._append_scheduling_power_events(power_events, scheduling)
        _append_new_task(power_events, new_task, new_task_start_time)

        # Sort power events by time
        power_events.sort(key=lambda d: d[0])

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
