import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches

matplotlib.use(matplotlib.get_backend()) #https://stackoverflow.com/a/52221178


class ColorPalette:

    def __init__(self, energy_edge, energy_face, task_edge, task_face, constant_boundary_edge,
                 constant_boundary_face, variable_boundary_edge, variable_boundary_face):
        self.energy_edge = energy_edge
        self.energy_face = energy_face
        self.task_edge = task_edge
        self.task_face = task_face
        self.constant_boundary_edge = constant_boundary_edge
        self.constant_boundary_face = constant_boundary_face
        self.variable_boundary_edge = variable_boundary_edge
        self.variable_boundary_face = variable_boundary_face


PALETTE_1 = ColorPalette(
    'green',
    'none',
    (0, 0, 1, 0.9),
    (0, 0, 1, 0.5),
    'gray',
    'gray',
    'red',
    'red',
)

# https://jfly.uni-koeln.de/color/
COLORBLIND_FRIENDLY_PALETTE = ColorPalette(
    (240/255.0, 228/255.0, 66/255.0),
    'none',
    (0, 0, 1, 0.9),
    (0, 0, 1, 0.5),
    'gray',
    'gray',
    (213/255.0, 94/255.0, 0/255.0),
    (213/255.0, 94/255.0, 0/255.0, 0.7),
)


class Drawer:

    BOUNDARY_HEIGHT = 5

    def __init__(self, height, width, color_palette=COLORBLIND_FRIENDLY_PALETTE, show_boundaries=True, show_legends=False):
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111)

        self.height = height
        self.width = width

        self.color_palette = color_palette
        self.show_boundaries = show_boundaries
        self.show_legends = True
        #self.language = 'eg'
        self.language = 'pt'

    def add_constant_boundary(self, start, width):
        rect = self._build_constant_boundary_rectangle((start, -Drawer.BOUNDARY_HEIGHT), width, Drawer.BOUNDARY_HEIGHT)
        self.ax.add_patch(rect)

    def add_variable_boundary(self, start, width):
        rect = self._build_variable_boundary_rectangle((start, -Drawer.BOUNDARY_HEIGHT), width, Drawer.BOUNDARY_HEIGHT)
        self.ax.add_patch(rect)

    def add_green_energy_availability(self, start, width, power):
        rect = self._build_green_energy_rectangle((start, 0), width, power)
        self.ax.add_patch(rect)

    def add_scheduled_task(self, start, task, y, include_task_id=True):
        rect = self._build_task_rectangle((start, y), task.runtime, task.power)

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

    def add_task(self, width, height, x, y, description=None):
        self._add_rectangle(width, height, x, y, self.color_palette.task_edge, self.color_palette.task_face, description=description)

    def _add_rectangle(self, width, height, x, y, edgecolor, facecolor, description=None):

        rect = patches.Rectangle((x, y), width, height, linewidth=1, edgecolor=edgecolor, facecolor=facecolor)

        if description:
            rx, ry = rect.get_xy()
            cx = rx + rect.get_width() / 2.0
            cy = ry + rect.get_height() / 2.0
            self.ax.annotate(description, (cx, cy), color='black', weight='bold', fontsize=10, ha='center', va='center')

        self.ax.add_patch(rect)

    def _build_constant_boundary_rectangle(self, xy, width, height):
        return patches.Rectangle(xy, width, height, linewidth=1, edgecolor=self.color_palette.constant_boundary_edge, facecolor=self.color_palette.constant_boundary_face)

    def _build_variable_boundary_rectangle(self, xy, width, height):
        return patches.Rectangle(xy, width, height, linewidth=1, edgecolor=self.color_palette.variable_boundary_edge, facecolor=self.color_palette.variable_boundary_face, hatch='x')

    def _build_green_energy_rectangle(self, xy, width, height):
        return patches.Rectangle(xy, width, height, linewidth=1, edgecolor=self.color_palette.energy_edge, facecolor=self.color_palette.energy_face, hatch='///')

    def _build_task_rectangle(self, xy, width, height):
        return patches.Rectangle(xy, width, height, linewidth=1, edgecolor=self.color_palette.task_edge, facecolor=self.color_palette.task_face)

    def _show_legend(self):
        width = 5
        heigh = 5

        markers = [
            self._build_task_rectangle((0, 0), width, heigh),
            self._build_green_energy_rectangle((0, 0), width, heigh),
        ]

        boundary_markers = [
            self._build_constant_boundary_rectangle((0, 0), width, heigh),
            self._build_variable_boundary_rectangle((0, 0), width, heigh),
        ]

        legend_text = [
            'Tarefa escalonada' if self.language == 'pt' else 'Scheduled Task',
            'Potência renovável' if self.language == 'pt' else 'Renewable Power',
        ]

        boundary_legend_text = [
            'Fronteira Constante' if self.language == 'pt' else 'Constant Boundary',
            'Fronteira Variável' if self.language == 'pt' else 'Variable Boundary',
        ]

        if self.show_boundaries:
            markers.extend(boundary_markers)
            legend_text.extend(boundary_legend_text)

        self.ax.legend(markers, legend_text)

    def _plot(self):
        y_start = -Drawer.BOUNDARY_HEIGHT if self.show_boundaries else 0
        if self.show_legends:
            self._show_legend()
        plt.xlim([0, self.width])
        plt.ylim([y_start, self.height + Drawer.BOUNDARY_HEIGHT])
        plt.xlabel('Tempo (s)' if self.language == 'pt' else 'Time (s)')
        plt.ylabel('Potência (W)' if self.language == 'pt' else 'Power (W)')

    def show(self):
        self._plot()
        plt.show()
        matplotlib.pyplot.close()

    def save(self, file):
        self._plot()
        plt.savefig(file)
        matplotlib.pyplot.close()
