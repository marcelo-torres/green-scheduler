from _csv import reader


class PhotovoltaReader:

    def __init__(self, resource_path):
        self.resource_path = resource_path

    def get_trace_1(self):
        file = 'photovolta_2016_part_1_0_and_1_1.csv'
        path = f'{self.resource_path}/photovolta/{file}'

        return self._get_tasks_energy_usage(path)

    def get_trace_2(self):
        file = 'photovolta_2016_part_1_2_and_1_3.csv'
        path = f'{self.resource_path}/photovolta/{file}'

        return self._get_tasks_energy_usage(path)

    def _get_tasks_energy_usage(self, source_file):

        with open(source_file, 'r') as file:
            csv_reader = reader(file)

            energy_usages = []
            for line, row in enumerate(csv_reader):
                if line == 0:
                    continue  # skip header
                energy_usage = float(row[2])
                energy_usage = int(energy_usage)
                energy_usages.append(energy_usage)

            return energy_usages
