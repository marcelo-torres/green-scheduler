from src.scheduling.model.machine_state import MachineState

CORES_PER_TASK = 1

class Machine:

    def __init__(self, id, cores=1, tdp=1):
        self.id = id
        self.cores = cores
        self.tdp = tdp
        self.state = MachineState(self)

    def schedule_task(self, task, start_time):
        # TODO Actually, each task uses just one core
        self.state.use_cores(start_time, task.runtime, CORES_PER_TASK)

    def unschedule_task(self, task, start_time):
        self.state.free_cores(start_time, task.runtime, CORES_PER_TASK)

    def can_schedule_task_in(self, task, start, end):
        return end - start >= task.runtime and self.state.min_free_cores_in(start, end) >= CORES_PER_TASK



