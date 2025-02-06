from src.scheduling.model.machine import Machine
from src.scheduling.model.power_series import PowerSeries


def create_single_machine_cluster(green_power, interval_length, cores=100):
    power_series = PowerSeries('g1', green_power, interval_length)
    machine = Machine('m1', cores, 0)
    return Cluster('c1', power_series, [machine])

class Cluster:

    def __init__(self, id, power_series, machines):
        self.id = id
        self.power_series = power_series
        self.machines = machines

