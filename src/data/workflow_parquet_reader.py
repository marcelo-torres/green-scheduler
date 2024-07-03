import random

import pandas as pd

from src.task_graph.task_graph import TaskGraph


class WorkflowTraceArchiveReader:

    def __init__(self, resource_path, min_power, max_power, seed=123456789):
        self.resource_path = resource_path
        self.min_power = min_power
        self.max_power = max_power
        self.seed = seed

    def epigenomics(self):
        trace = 'workflowhub_epigenomics_dataset-taq_chameleon-cloud_schema-0-2_epigenomics-taq-100000-cc-run002_parquet'
        path = self.get_path(trace)
        return self._create_graph(path, self.min_power, self.max_power, self.seed)

    def montage(self):
        trace = 'workflowhub_montage_ti01-971107n_degree-4-0_osg_schema-0-2_montage-4-0-osg-run009_parquet'
        path = self.get_path(trace)
        return self._create_graph(path, self.min_power, self.max_power, self.seed)

    def get_path(self, trace):
        return f'{self.resource_path}/workflow_trace_archive/{trace}/tasks/schema-1.0/part.0.parquet'

    def _generate_random_power(self, min, max, seed):
        random.seed(seed)
        return random.uniform(min, max)

    def _create_graph(self, task_file, min_power, max_power, seed):
        df = pd.read_parquet(task_file, engine='pyarrow')

        graph = TaskGraph()

        start_task_id = 0
        graph.add_new_task(start_task_id, runtime=0, power=0)  # Dummy task
        graph.set_start_task(start_task_id)

        for index, row in df.iterrows():
            runtime = int(row['runtime'] / 1000) # milliseconds to seconds

            power = self._generate_random_power(min_power, max_power, seed + row['id'])
            #power = self._generate_random_power(min_power, max_power, seed)
            graph.add_new_task(row['id'], runtime=runtime, power=power)

        for index, row in df.iterrows():

            parent = row['id']
            children = row['children']

            if len(row['parents']) == 0:
                graph.create_dependency(start_task_id, parent)


            for child in children:
                graph.create_dependency(parent, child)

        return graph
