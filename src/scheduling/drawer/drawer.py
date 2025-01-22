import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches

matplotlib.use(matplotlib.get_backend()) #https://stackoverflow.com/a/52221178


class Drawer:

    BOUNDARY_HEIGHT = 5

    def __init__(self, height, width):
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111)

        self.height = height
        self.width = width

    def add_constant_boundary(self, start, width):
        rect = patches.Rectangle((start, -Drawer.BOUNDARY_HEIGHT), width, Drawer.BOUNDARY_HEIGHT, linewidth=1, edgecolor='gray', facecolor='gray')
        self.ax.add_patch(rect)

    def add_variable_boundary(self, start, width):
        rect = patches.Rectangle((start, -Drawer.BOUNDARY_HEIGHT), width, Drawer.BOUNDARY_HEIGHT, linewidth=1, edgecolor='red', facecolor='red')
        self.ax.add_patch(rect)

    def add_green_energy_availability(self, start, width, power):
        rect = patches.Rectangle((start, 0), width, power, linewidth=1, edgecolor='green', facecolor='none')
        self.ax.add_patch(rect)

    def add_scheduled_task(self, start, task, y, include_task_id=True):
        edgecolor = (0, 0, 1, 0.9)
        facecolor = (0, 0, 1, 0.5)

        rect = patches.Rectangle((start, y), task.runtime, task.power, linewidth=1, edgecolor=edgecolor, facecolor=facecolor)

        if include_task_id:
            rx, ry = rect.get_xy()
            cx = rx + rect.get_width() / 2.0
            cy = ry + rect.get_height() / 2.0
            self.ax.annotate(task.id, (cx, cy), color='black', weight='bold', fontsize=10, ha='center', va='center')

        self.ax.add_patch(rect)
        self.ax.autoscale(False)

    def add_line(self, x_list, y_list, color='blue'):
        plt.plot(x_list, y_list, color, lw=1)

    def add_single_line(self, width, x, y):
        plt.plot([x, x+width], [y, y], 'blue', lw=1)

    def add_rectangle(self, width, height, x, y, description=None, edgecolor=(0, 0, 1, 0.9), facecolor=(0, 0, 1, 0.5)):

        rect = patches.Rectangle((x, y), width, height, linewidth=1, edgecolor=edgecolor, facecolor=facecolor)

        if description:
            rx, ry = rect.get_xy()
            cx = rx + rect.get_width() / 2.0
            cy = ry + rect.get_height() / 2.0
            self.ax.annotate(description, (cx, cy), color='black', weight='bold', fontsize=10, ha='center', va='center')

        self.ax.add_patch(rect)

    def _plot(self):
        plt.xlim([0, self.width])
        plt.ylim([-Drawer.BOUNDARY_HEIGHT, self.height + Drawer.BOUNDARY_HEIGHT])
        plt.xlabel('Time (s)')
        plt.ylabel('Power (W)')

    def show(self):
        self._plot()
        plt.show()
        matplotlib.pyplot.close()

    def save(self, file):
        self._plot()
        plt.savefig(file)
        matplotlib.pyplot.close()
