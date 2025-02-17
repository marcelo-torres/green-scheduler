from src.scheduling.model.machine import Machine
from src.scheduling.model.machine_state import MachineState
from src.scheduling.model.power_series import PowerSeries


def create_single_machine_cluster(green_power, interval_length, cores=100):
    power_series = PowerSeries('g1', green_power, interval_length)
    machine = Machine('m1', cores, 0)
    return Cluster('c1', power_series, [machine])


def _create_machine_map(machines):
    machines_map = {}

    for machine in machines:
        if machine.id in machines_map:
            raise Exception(f'Machine id={machine.id} must be unique in a cluster')
        machines_map[machine.id] = machine

    return machines_map


class Cluster:

    def __init__(self, id, power_series, machines):
        self.id = id
        self.power_series = power_series
        self.machines_list = machines
        self.machines = _create_machine_map(machines)
