import queue
from multiprocessing import JoinableQueue, Process


class ParallelExperimentExecutor:

    def __init__(self, save_report):
        self.save_report = save_report
        self.reports_queue = None
        self.report_handler = None
        self.workers = []

    def start(self):
        self.reports_queue = JoinableQueue()
        self.report_handler = Process(target=self._handle_report, daemon=True)
        self.report_handler.start()

    def run_experiment_async(self, experiment):
        worker = Process(target=lambda: self._execute_experiment(experiment), daemon=True)
        worker.start()
        self.workers.append(worker)

    def wait_all(self):
        joined_workers = []
        for worker in self.workers:
            worker.join()
            joined_workers.append(worker)

        for worker in joined_workers:
            self.workers.remove(worker)

    def stop(self):
        self.wait_all()
        self.report_handler.kill()

    def _handle_report(self):
        while True:
            try:
                report = self.reports_queue.get(block=False)
                self.save_report(report)
                self.reports_queue.task_done()

            except queue.Empty:
                pass

    def _execute_experiment(self, experiment):
        report = experiment()
        self.reports_queue.put(report)
        self.reports_queue.join()
