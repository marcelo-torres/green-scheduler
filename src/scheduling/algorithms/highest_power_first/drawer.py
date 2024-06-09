import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches

matplotlib.use("qt5agg") #https://stackoverflow.com/a/52221178


class Drawer:

    BOUNDARY_HEIGHT = 5

    def __init__(self, height, width):
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111)

        self.height = height
        self.width = width

    def add_constant_boundary(self, start, width):
        rect = patches.Rectangle((start, 0), width, Drawer.BOUNDARY_HEIGHT, linewidth=1, edgecolor='gray', facecolor='gray')
        self.ax.add_patch(rect)

    def add_variable_boundary(self, start, width):
        rect = patches.Rectangle((start, 0), width, Drawer.BOUNDARY_HEIGHT, linewidth=1, edgecolor='red', facecolor='red')
        self.ax.add_patch(rect)

    def add_green_energy_availability(self, start, width, power):
        rect = patches.Rectangle((start, Drawer.BOUNDARY_HEIGHT), width, power, linewidth=1, edgecolor='green', facecolor='none')
        self.ax.add_patch(rect)

    def add_scheduled_task(self, start, task, y):
        rect = patches.Rectangle((start, Drawer.BOUNDARY_HEIGHT + y), task.runtime, task.power, linewidth=1, edgecolor='blue', facecolor='blue')
        rx, ry = rect.get_xy()
        cx = rx + rect.get_width() / 2.0
        cy = ry + rect.get_height() / 2.0
        self.ax.annotate(task.id, (cx, cy), color='black', weight='bold', fontsize=10, ha='center', va='center')
        self.ax.add_patch(rect)

    def add_task(self, start, task, y):
        rect = patches.Rectangle((start, Drawer.BOUNDARY_HEIGHT + y), task.runtime, task.power, linewidth=1, edgecolor='yellow', facecolor='yellow')
        self.ax.add_patch(rect)

    def show(self):
        plt.xlim([0, self.width])
        plt.ylim([0, self.height])
        plt.show()
