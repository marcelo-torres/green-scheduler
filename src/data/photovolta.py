from _csv import reader
from statistics import median


def _get_green_power_from(source_file, size=1):

    with open(source_file, 'r') as file:
        csv_reader = reader(file)

        powers = []
        for line, row in enumerate(csv_reader):
            if line == 0:
                continue  # skip header
            energy_usage = float(row[2]) * size
            energy_usage = int(energy_usage)
            powers.append(energy_usage)

        return powers


class PhotovoltaReader:

    def __init__(self, resource_path):
        self.resource_path = resource_path

    def get_trace_1(self, size=1):
        file = 'photovolta_2016_part_1_0_and_1_1.csv'
        path = f'{self.resource_path}/photovolta/{file}'
        return _get_green_power_from(path, size=size)

    def get_trace_2(self, size=20):
        file = 'photovolta_2016_part_1_2_and_1_3.csv'
        path = f'{self.resource_path}/photovolta/{file}'
        return _get_green_power_from(path, size=size)

    def get_trace_3(self, size=20):
        file = 'photovolta_2016_part_2_0_and_2_1.csv'
        path = f'{self.resource_path}/photovolta/{file}'
        return _get_green_power_from(path, size=size)

    def get_trace_4(self, size=20):
        file = 'photovolta_2016_part_1_7_and_1_8.csv'
        path = f'{self.resource_path}/photovolta/{file}'
        return _get_green_power_from(path, size=size)

    def get_trace_5(self, size=20):
        file = 'photovolta_2016_part_2_4_and_2_5.csv'
        path = f'{self.resource_path}/photovolta/{file}'
        return _get_green_power_from(path, size=size)

    def stats(self, power_trace):
        total_power = 0
        for power in power_trace:
            total_power += power

        average_power = float(total_power) / len(power_trace)

        return average_power, median(power_trace)
