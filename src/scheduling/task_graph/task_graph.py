from src.scheduling.task_graph.task import Task


class TaskGraph:

    def __init__(self):
        self.start_task_id = None
        self.tasks = {}

    def set_start_task(self, task_id):
        self.start_task_id = task_id

    def add_new_task(self, task_id, runtime=None, power=None):
        if task_id in self.tasks:
            raise Exception(f"Task ${task_id} already exists")

        task = Task(task_id, runtime, power)
        self.tasks[task_id] = task

    def get_task(self, task_id):
        return self.tasks[task_id]

    def get_first_task(self):
        return self.get_task(
            self.start_task_id
        )

    def create_dependency(self, task_a_id, task_b_id):
        task_a = self.tasks[task_a_id]
        task_b = self.tasks[task_b_id]

        task_a.successors.append(task_b)
        task_b.predecessors.append(task_a)

    def remove_task(self, task_id):
        task = self.tasks[task_id]
        for succ in task.successors:
            succ.predecessors.remove(task)

        for pred in task.predecessors:
            pred.successors.remove(task)

        del self.tasks[task_id]


    def list_of_tasks(self):
        return list(
            self.tasks.values()
        )
