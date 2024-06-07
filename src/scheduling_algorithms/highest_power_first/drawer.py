import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches

matplotlib.use("qt5agg") #https://stackoverflow.com/a/52221178

class Drawer:

    def __init__(self):
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111)

    def add_constant_boundary(self, start, width):
        rect = patches.Rectangle((start, 50), width, 5, linewidth=1, edgecolor='gray', facecolor='gray')
        self.ax.add_patch(rect)

    def add_variable_boundary(self, start, width):
        rect = patches.Rectangle((start, 55), width, 5, linewidth=1, edgecolor='red', facecolor='red')
        self.ax.add_patch(rect)

    def add_green_energy_availability(self, start, width, power):
        rect = patches.Rectangle((start, 0), width, power, linewidth=1, edgecolor='green', facecolor='none')
        self.ax.add_patch(rect)


    def add_scheduled_task(self, start, duration):
        rect = patches.Rectangle((start, 0), duration, 3, linewidth=1, edgecolor='blue', facecolor='blue')
        self.ax.add_patch(rect)

    def add_task(self, start, duration):
        rect = patches.Rectangle((start, 0), duration, 3, linewidth=1, edgecolor='yellow', facecolor='yellow')
        self.ax.add_patch(rect)

    def show(self):
        plt.xlim([0, 130])
        plt.ylim([0, 130])
        plt.show()
        pass

