import matplotlib
import matplotlib.pyplot as plt

matplotlib.use(matplotlib.get_backend()) #https://stackoverflow.com/a/52221178


class ActiveTasksDrawer:
    def __init__(self):
        self.fig = plt.figure()

    def draw(self, active_tasks_by_time):
        x = []
        y = []

        for event_time, active_tasks_count in active_tasks_by_time:
            x.append(event_time)
            y.append(active_tasks_count)

        plt.plot(x, y, color='red')
        plt.xlabel('Time')
        plt.ylabel('Active tasks')
        plt.title('Active Tasks by time')
        plt.show()
