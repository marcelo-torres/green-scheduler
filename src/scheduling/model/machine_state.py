import pyredblack
from bintrees import AVLTree


class MachineState:

    def __init__(self, machine):
        self.machine = machine

        self.events = AVLTree()
        self.events.insert(0, machine.cores)
        self.events.insert(float('inf'), machine.cores)

    def use_cores(self, start, duration, amount):

        if duration == 0:
            return

        end = start + duration

        if amount > self.min_free_cores_in(start, end):
            raise Exception(f'There is not enough cores to use between {start} and {end}')

        last_cores_available = -1

        # TreeSlice[s1:e1] -> TreeSlice object, with keys in range s1 <= key < e1
        current_events = list(self.events[start:end].items())
        if len(current_events) == 0 or start not in self.events:
            previous_time = self.events.floor_key(start)
            current_available_cores = self.events[previous_time] - amount
            self._validate_new_cores_used(current_available_cores, amount)
            self.events[start] = current_available_cores
            last_cores_available = current_available_cores

        for time, cores in current_events:
            current_available_cores = cores - amount
            self._validate_new_cores_used(current_available_cores, amount)
            self.events[time] = current_available_cores
            last_cores_available = current_available_cores

        if end not in self.events:
            current_available_cores = last_cores_available + amount
            self._validate_new_cores_used(current_available_cores, amount)
            self.events[end] = current_available_cores

    def free_cores(self, start, duration, amount):
        end = start + duration

        last_cores_available = float('-inf')

        # Create start event
        if start not in self.events:
            previous_time = self.events.floor_key(start)  # key <= e1
            cores = self.events[previous_time]
            current_cores_available = cores + amount
            self._validate_new_free_cores(current_cores_available)
            self.events[start] = current_cores_available
            last_cores_available = cores

            current_events = list(self.events[start+1:end].items())  # s1+1 <= key < e1 => s1 < key < e1
        else:
            current_events = list(self.events[start:end].items())  # s1 <= key < e1 => s1 <= key < e1

        # Update events
        for time, cores in current_events:
            current_cores_available = cores + amount
            self._validate_new_free_cores(current_cores_available)
            self.events[time] = current_cores_available
            last_cores_available = cores

        # Create end event
        if end not in self.events:
            self.events[end] = last_cores_available

    def min_free_cores_in(self, start, end):
        previous_time = self.events.floor_key(start)

        if start == end:
            return self.events[previous_time]

        min_cores = float('inf')
        for time, available_cores in self.events[previous_time:end].items():
            if available_cores < min_cores:
                min_cores = available_cores

        return min_cores

    def next_start(self, current_start):
        return self.events.ceiling_key(current_start+1)

    def previous_start(self, current_start):
        return self.events.floor_key(current_start-1)

    def search_intervals_with_free_cores(self, start, end, min_duration, min_cores):

        events_iterator = self.events.iter_items(self.events.floor_key(start), end)

        def next_event():
            return next(events_iterator, (None, -1))

        e, cores = next_event()
        if e < start:
            e = start

        i_start = e

        while e is not None:

            # Increase the interval while it has enough cores
            if cores >= min_cores:
                e, cores = next_event()
                continue

            # Return the interval if the interval length is greater or equal to min duration
            i_length = e - i_start
            if i_length >= min_duration:
                yield i_start, e

            # Next interval and next start
            e, cores = next_event()
            i_start = e

        if i_start is not None:
            i_length = end - i_start
            if i_length >= min_duration:
                yield i_start, end

    def _validate_new_cores_used(self, current_cores_available, amount):
        if current_cores_available < 0:
            raise Exception(f' Attempted to use {amount} cores, but machine cannot have {current_cores_available} free cores!')

    def _validate_new_free_cores(self, current_cores_available):
        if current_cores_available > self.machine.cores:
            raise Exception(f'A machine with {self.machine.cores} cores cannot have {current_cores_available} free cores!')
