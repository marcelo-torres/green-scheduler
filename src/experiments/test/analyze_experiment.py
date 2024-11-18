import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.lines as mlines

workflows = [
    'blast',
     'bwa',
     'cycles',
     'genome',
    'soykb',
    'srasearch',
    'montage',
    'seismology',
]

traces = [
    'trace-1',
    'trace-2',
    'trace-3',
    'trace-4',
]


class Mapper:
    def __init__(self, mapping, name, get_color_and_marker):
        self.mapping = mapping
        self.name = name
        self._get_color_and_marker = get_color_and_marker

    def get_color_and_marker(self, df_row):
        return self._get_color_and_marker(df_row, self.mapping)


deadline_factor_map = Mapper(
    {
        2: ('red', '.'),
        4: ('blue', '.'),
        8: ('green', '.')
    },
    'deadline_factor',
    lambda df_row, mapping: mapping[df_row.deadline_factor]
)

c_values_map = Mapper(
    {
        0: ('red', '.'),
        0.5: ('blue', '.'),
        0.8: ('green', '.')
    },
    'c_value',
    lambda df_row, mapping: mapping[df_row.c_value]
)

shift_modes_map = Mapper(
    {
        'none': ('red', '.'),
        'left': ('blue', '.'),
        'right-left': ('green', '.')
    },
    'shift_mode',
    lambda df_row, mapping: mapping[df_row.shift_mode]
)

task_ordering_map = Mapper(
    {
        'energy': ('red', '.'),
        'power': ('blue', '.'),
        'runtime': ('green', '.'),
        'runtime_ascending': ('orange', '.'),
    },
    'task_ordering',
    lambda df_row, mapping: mapping[df_row.task_ordering]
)

best_map = Mapper(
    {
        '0.8_right-left_8_runtime': ('green', '+'),
        '0.0_left_2_runtime_ascending': ('blue', '*'),
        '0.8_right-left_8_energy': ('orange', 'o'),
    },
    '_',
    lambda df_row, mapping: mapping[best_key(df_row)] if best_key(df_row) in mapping else ('gray', '.'),
)


def best_key(df_row):
    return f'{df_row.c_value}_{df_row.shift_mode}_{df_row.deadline_factor}_{df_row.task_ordering}'


best_2_map = Mapper(
    {
        '0.8_right-left_8': ('green', 'o'),
        '0.0_left_2': ('blue', '*'),
    },
    '_',
    lambda df_row, mapping: mapping[best_2_key(df_row)] if best_2_key(df_row) in mapping else ('gray', '.'),
)


def best_2_key(df_row):
    return f'{df_row.c_value}_{df_row.shift_mode}_{df_row.deadline_factor}'


best_3_map = Mapper(
    {
        'right-left_8_runtime': ('green', '+'),
        'right-left_8_runtime_ascending': ('blue', '*'),
        'right-left_8_energy': ('orange', 'o'),
        'right-left_8_power': ('red', 'o'),
    },
    '_',
    lambda df_row, mapping: mapping[best_3_key(df_row)] if best_3_key(df_row) in mapping else ('gray', '.'),
)

def best_3_key(df_row):
    return f'{df_row.shift_mode}_{df_row.deadline_factor}_{df_row.task_ordering}'


def load_data():
    resources_path = '../../../resources/'
    relative_path = 'experiments-shift/experiments_2024-10-30_01-58-50/'
    file_name = 'report_experiments_2024-10-30_01-58-50_data_aggregation.csv'

    file = f'{resources_path}{relative_path}{file_name}'
    return pd.read_csv(file)


def get_canvas_workflow_by_trace(df, color_and_marker_getter=lambda x:('red', 'o')):

    n_rows = 2
    n_cols = 2

    assert n_rows * n_rows >= len(traces), 'The number of subplots must be greater or equal to the number of traces'

    fig = plt.figure()
    fig.set_size_inches(10, 10)

    gs = fig.add_gridspec(n_rows, n_cols, hspace=0, wspace=0)
    axs = gs.subplots(sharex='col', sharey='row')

    g_row = 0
    g_col = 0

    for trace in traces:

        df_workflow = df[df['energy_trace'] == trace]

        ax = axs[g_row, g_col]

        ax.text(0.98, 0.98, trace,
                transform=ax.transAxes,  # Use axes coordinates
                fontsize=14,  # Font size of the title
                verticalalignment='top',  # Align text to the top
                horizontalalignment='right',  # Align text to the right
                bbox=dict(facecolor='white', alpha=0.6))

        if g_row == 0:
            ax.set_xlabel('Makespan (minutes)')

        if g_col == n_cols - 1:
            ax.set_ylabel('Brown energy (kJ)')

        for row in df_workflow.itertuples(index=True, name='Pandas'):
            makespan = row.AVERAGE_of_makespan / 60
            brown_energy = row.AVERAGE_of_brown_energy_used / 1000

            color, marker = color_and_marker_getter(row)
            size = 25
            if color != 'gray':
                size = 90
            ax.scatter(makespan, brown_energy, color=color, marker=marker, s=size)

        # Update next g_row and g_col
        g_col += 1
        if g_col % n_cols == 0:
            g_col = 0
            g_row += 1

    fig.tight_layout(pad=0)
    canvas_fig = FigureCanvas(fig)
    plt.close(fig)

    return canvas_fig


def plot_all_workflows(mapper, power_distribution, legend=False):

    n_rows = 2
    n_cols = 4
    assert n_rows * n_cols >= len(workflows), 'The number of subplots must be greater or equal to the number of workflows'

    df = load_data()
    if power_distribution is not None:
        df = df[df['power_distribution'] == power_distribution]

    fig, ax = plt.subplots(n_rows, n_cols, figsize=(10, 5))

    g_row = 0
    g_col = 0

    for workflow in workflows:
        df_workflow = df[df['workflow'] == workflow]
        canvas_fig = get_canvas_workflow_by_trace(df_workflow, color_and_marker_getter=mapper.get_color_and_marker)

        canvas_fig.draw()
        img = np.frombuffer(canvas_fig.get_renderer().tostring_rgb(), dtype='uint8').reshape(
            canvas_fig.get_width_height()[::-1] + (3,))

        current_ax = ax[g_row][g_col]
        current_ax.imshow(img)
        current_ax.axis('off')
        current_ax.set_title(workflow)

        # Update next g_row and g_col
        g_col += 1
        if g_col % n_cols == 0:
            g_col = 0
            g_row += 1

    # Add legend
    if legend:
        handles = []
        for label, d in mapper.mapping.items():
            color, shape = d
            handle = mlines.Line2D([], [], color=color, marker=shape, linestyle='None', markersize=10, label=label)
            handles.append(handle)
        plt.legend(handles=handles, loc='upper left', bbox_to_anchor=(-1.8, -0.5), ncol=3)

    #plt.legend(handles=handles, loc='upper center', bbox_to_anchor=(-1, -0.1), ncol=len(mapper.mapping))
    fig.tight_layout(pad=0)
    plt.show()


def shows_canvas_figs(n_rows, n_cols, canvas_figs):
    assert n_rows * n_cols >= len(canvas_figs), 'The number of subplots must be greater or equal to the number of canvas_figs'

    fig, ax = plt.subplots(n_rows, n_cols, figsize=(10, 5))

    g_row = 0
    g_col = 0

    for title, canvas_fig in canvas_figs:
        canvas_fig.draw()
        img = np.frombuffer(canvas_fig.get_renderer().tostring_rgb(), dtype='uint8').reshape(
            canvas_fig.get_width_height()[::-1] + (3,))

        current_ax = ax[g_row][g_col]
        current_ax.imshow(img)
        current_ax.axis('off')
        current_ax.set_title(title)

        # Update next g_row and g_col
        g_col += 1
        if g_col % n_cols == 0:
            g_col = 0
            g_row += 1

    fig.tight_layout(pad=0)
    plt.show()

def plot_legend_only(mapper):
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))

    handles = []
    labels = mapper.mapping.items()
    for label, d in labels:
        color, shape = d
        handle = mlines.Line2D([], [], color=color, marker=shape, linestyle='None', markersize=10, label=label)
        handles.append(handle)
    plt.legend(handles=handles, loc='center', ncol=len(labels))
    ax.axis('off')
    plt.show()


def analyze_top_solutions():
    df = load_data()

    properties = [deadline_factor_map, c_values_map, shift_modes_map, task_ordering_map]
    properties = [shift_modes_map]

    df = df[df['power_distribution'] == 'uniform']

    df['AVERAGE_of_brown_energy_used'] = df['AVERAGE_of_brown_energy_used'] / 1000
    df['AVERAGE_of_makespan'] = df['AVERAGE_of_makespan']/60

    for trace in traces:
        df_trace = df[df['energy_trace'] == trace]

        canvas_figs = []

        for workflow in workflows:
            df_workflow = df_trace[df_trace['workflow'] == workflow]

            for prop in properties:

                n_rows = 1
                n_cols = len(prop.mapping.items())

                fig = plt.figure()
                fig.set_size_inches(10, 10)

                gs = fig.add_gridspec(n_rows, n_cols, hspace=0, wspace=0)
                axs = gs.subplots(sharex='col', sharey='row')

                i = 0
                for value, d in prop.mapping.items():
                    color, shape = d

                    df_prop = df_workflow[df_workflow[prop.name] == value]

                    ax = axs[i]
                    #ax.hist(df_prop['AVERAGE_of_brown_energy_used'], color=color)
                    #AVERAGE_of_makespan

                    max = df_workflow.AVERAGE_of_makespan.max()
                    ax.set_xlim(0, max+100)
                    ax.hist(df_prop['AVERAGE_of_makespan'], color=color)
                    ax.set_title(f'{prop.name}={value}')
                    i += 1

                fig.tight_layout(pad=0)
                canvas_fig = FigureCanvas(fig)
                plt.close(fig)

                title = f'{trace} - {workflow}'
                canvas_figs.append(
                    (title, canvas_fig)
                )

        shows_canvas_figs(2, 4, canvas_figs)





            # smallest_brown_energy = df_workflow.AVERAGE_of_brown_energy_used.min()
            # print(smallest_brown_energy[0]['AVERAGE_of_brown_energy_used'])

            # smallests = df_workflow.nsmallest(50, 'AVERAGE_of_brown_energy_used')
            #
            # for smallest in smallests.itertuples(index=True, name="Row"):
            #     print(f'{trace}\t{workflow}\tBrownEnergy={round(smallest.AVERAGE_of_brown_energy_used, 6)}\t'
            #           + f'c_value={smallest.c_value}\tdeadline_factor={smallest.deadline_factor}\tshift_mode={smallest.shift_mode}\ttask_ordering={smallest.task_ordering}')


if __name__ == '__main__':
    # plot_all_workflows(deadline_factor_map)
    # plot_all_workflows(c_values_map)
    # plot_all_workflows(shift_modes_map)
    # plot_all_workflows(task_ordering_map)

    # gaussian  inverted_exponential   uniform



    #plot_all_workflows(best_map, 'uniform')


    #plot_all_workflows(task_ordering_map, 'inverted_exponential')

    #plot_legend_only(shift_modes_map)

    #analyze_top_solutions()

    plot_all_workflows(best_3_map, 'uniform')
    plot_legend_only(best_3_map)