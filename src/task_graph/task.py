class Task:

    def __init__(self, task_id, runtime=None, power=None):
        self.id = task_id
        self.runtime = runtime
        self.power = power
        self.successors = []
        self.predecessors = []

    def __str__(self):
        return str(
            self.id
        )
