import pyredblack
from bintrees import AVLTree


class MachineState:

    def __init__(self, machine, deadline):
        self.machine = machine

        self.events = AVLTree()
        self.events.insert(0, machine.cores)
        self.events.insert(deadline, machine.cores)

    def use_cores(self, start, duration, amount):

        end = start + duration
        if amount > self.min_free_cores_in(start, end):
            raise Exception(f'There is not enough cores to use between {start} and {end}')

        last_cores_available = -1

        # TreeSlice[s1:e1] -> TreeSlice object, with keys in range s1 <= key < e1
        current_events = list(self.events[start:end].items())
        if len(current_events) == 0 or current_events[0][0] is not start: # TODO  teste or
            previous_time = self.events.floor_key(start)
            current_available_cores = self.events[previous_time] - amount
            self.events[start] = current_available_cores
            last_cores_available = current_available_cores

        for time, cores in current_events:
            self.events[time] = cores - amount
            last_cores_available = cores - amount

        if end not in self.events:
            self.events[end] = last_cores_available + amount

    def free_cores(self, start, duration, amount): # TODO teste
        end = start + duration

        last_time = self.events.ceiling_key(end)
        current_events = list(self.events[start:last_time].items())

        last_cores_available = -1
        if len(current_events) > 0 and current_events[0][0] is not start:
            last_cores_available = current_events[0][1]
            current_events.pop()

        if len(current_events) == 0:
            self.events[start] = amount
        elif current_events[0][0] is not start:
            self.events[start] = last_cores_available + amount

        for time, cores in current_events:
            self.events[time] = cores + amount

    def min_free_cores_in(self, start, end):
        previous_time = self.events.floor_key(start)

        min_cores = float('inf')
        for time, available_cores in self.events[previous_time:end].items():
            if available_cores < min_cores:
                min_cores = available_cores

        return min_cores
