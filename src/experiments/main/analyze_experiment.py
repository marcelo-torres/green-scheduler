import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.lines as mlines
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec

resources_path = '../../../resources/'

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

shift_modes = ['none', 'left', 'right-left']
deadline_factors = [2, 4, 8]
c_values = [0.0, 0.5, 0.8]
task_ordering_criterias = ['energy', 'power', 'runtime', 'runtime_ascending']


def parse_key_factor(deadline_factor):
    return f'Factor {deadline_factor}'

def parse_key_shift(shift_mode):
    if shift_mode == 'none':
        return 'No-shift'
    else:
        return f'Shift-{shift_mode}'

def parse_key_task_ordering(task_ordering, c_value):
    friendly_name_map = {
        'energy': 'Energy',
        'power': 'Power',
        'runtime': 'LPT',
        'runtime_ascending': 'SPT'
    }
    friendly_name = friendly_name_map[task_ordering]
    return f'{friendly_name} c={c_value}'



class Mapper:
    def __init__(self, mapping, name, get_color_and_marker):
        self.mapping = mapping
        self.name = name
        self._get_color_and_marker = get_color_and_marker

    def get_color_and_marker(self, df_row):
        return self._get_color_and_marker(df_row, self.mapping)


DEFAULT_SHAPE_SIZE = 35

deadline_factor_map = Mapper(
    {
        2: ('red', '.', DEFAULT_SHAPE_SIZE),
        4: ('blue', '.', DEFAULT_SHAPE_SIZE),
        8: ('green', '.', DEFAULT_SHAPE_SIZE)
    },
    'deadline_factor',
    lambda df_row, mapping: mapping[df_row.deadline_factor]
)

c_values_map = Mapper(
    {
        0.0: ('red', '.', DEFAULT_SHAPE_SIZE),
        0.5: ('blue', '.', DEFAULT_SHAPE_SIZE),
        0.8: ('green', '.', DEFAULT_SHAPE_SIZE)
    },
    'c_value',
    lambda df_row, mapping: mapping[df_row.c_value]
)

shift_modes_map = Mapper(
    {
        'none': ('red', '.', DEFAULT_SHAPE_SIZE),
        'left': ('blue', '.', DEFAULT_SHAPE_SIZE),
        'right-left': ('green', '.', DEFAULT_SHAPE_SIZE)
    },
    'shift_mode',
    lambda df_row, mapping: mapping[df_row.shift_mode]
)

task_ordering_map = Mapper(
    {
        'energy': ('red', '.', DEFAULT_SHAPE_SIZE),
        'power': ('blue', '.', DEFAULT_SHAPE_SIZE),
        'runtime': ('green', '.', DEFAULT_SHAPE_SIZE),
        'runtime_ascending': ('orange', '.', DEFAULT_SHAPE_SIZE),
    },
    'task_ordering',
    lambda df_row, mapping: mapping[df_row.task_ordering]
)

best_map = Mapper(
    {
        '0.8_right-left_8_runtime': ('green', '+', DEFAULT_SHAPE_SIZE),
        '0.0_left_2_runtime_ascending': ('blue', '*', DEFAULT_SHAPE_SIZE),
        '0.8_right-left_8_energy': ('orange', 'o', DEFAULT_SHAPE_SIZE),
    },
    '_',
    lambda df_row, mapping: mapping[best_key(df_row)] if best_key(df_row) in mapping else ('gray', '.', DEFAULT_SHAPE_SIZE),
)


def best_key(df_row):
    return f'{df_row.c_value}_{df_row.shift_mode}_{df_row.deadline_factor}_{df_row.task_ordering}'


best_2_map = Mapper(
    {
        '0.8_right-left_8': ('green', 'o', DEFAULT_SHAPE_SIZE),
        '0.0_left_2': ('blue', '*', DEFAULT_SHAPE_SIZE),
    },
    '_',
    lambda df_row, mapping: mapping[best_2_key(df_row)] if best_2_key(df_row) in mapping else ('gray', '.'),
)


def best_2_key(df_row):
    return f'{df_row.c_value}_{df_row.shift_mode}_{df_row.deadline_factor}'


best_3_map = Mapper(
    {
        'right-left_8_runtime': ('green', '+', DEFAULT_SHAPE_SIZE),
        'right-left_8_runtime_ascending': ('blue', '*', DEFAULT_SHAPE_SIZE),
        'right-left_8_energy': ('orange', 'o', DEFAULT_SHAPE_SIZE),
        'right-left_8_power': ('red', 'o', DEFAULT_SHAPE_SIZE),
    },
    '_',
    lambda df_row, mapping: mapping[best_3_key(df_row)] if best_3_key(df_row) in mapping else ('gray', '.', DEFAULT_SHAPE_SIZE),
)


def best_3_key(df_row):
    return f'{df_row.shift_mode}_{df_row.deadline_factor}_{df_row.task_ordering}'


def _build_all_map():
    shift_symbols = {
        shift_modes[0]: 'x',
        shift_modes[1]: '<',
        shift_modes[2]: '>'
    }

    tasks_ordering_and_c_values_colors = {
        task_ordering_criterias[0]: {
            c_values[0]: 'gray', c_values[1]: 'yellow', c_values[2]: 'black',
        },
        task_ordering_criterias[1]: {
                c_values[0]: 'red', c_values[1]: 'lightcoral', c_values[2]: 'orange',
            },
        task_ordering_criterias[2]: {
                c_values[0]: 'green', c_values[1]: 'lime', c_values[2]: 'turquoise',
            },
        task_ordering_criterias[3]: {
                c_values[0]: 'blue', c_values[1]: 'blueviolet', c_values[2]: 'magenta',
            },
    }

    deadline_factor_sizes = {
        deadline_factors[0]: 20,
        deadline_factors[1]: 50,
        deadline_factors[2]: 110,
    }

    def _build_legend_lines():
        legend_lines = {}

        for deadline_factor, size in deadline_factor_sizes.items():
            deadline_key = parse_key_factor(deadline_factor)
            legend_lines[deadline_key] = ('black', 'o', size)

        for shift_mode, symbols in shift_symbols.items():
            shift_key = parse_key_shift(shift_mode)
            legend_lines[shift_key] = ('black', symbols, DEFAULT_SHAPE_SIZE)

        for task_ordering, color_tone_mapping in tasks_ordering_and_c_values_colors.items():
            for c_value, color in color_tone_mapping.items():

                task_ordering_key = parse_key_task_ordering(task_ordering, c_value)

                legend_lines[task_ordering_key] = (color, 'o', DEFAULT_SHAPE_SIZE)

        return legend_lines

    def _build_all_map():
        map_temp = {}

        for shift_mode in shift_modes:
            mark = shift_symbols[shift_mode]

            for deadline in deadline_factors:
                size = deadline_factor_sizes[deadline]

                for task_ordering in task_ordering_criterias:
                    color_tone = tasks_ordering_and_c_values_colors[task_ordering]

                    for c_value in c_values:
                        key = f'{shift_mode}_{deadline}_{c_value}_{task_ordering}'
                        color = color_tone[c_value]
                        map_temp[key] = (color, mark, size)
        return map_temp

    return _build_legend_lines(), _build_all_map()


legend_lines, all_map_data = _build_all_map()


all_map = Mapper(
    all_map_data,
    '_',
    lambda df_row, mapping: mapping[all_key(df_row)] if all_key(df_row) in mapping else ('gray', '.', 300),
)


def _add_to_mapping_list(ax, key, mapping):
    if key not in mapping:
        mapping[key] = []
    mapping[key].append(ax)


class AxeVisibilityController:

    def __init__(self):
        self.axes_keys = {}
        self.key_mapping = {}

    def add(self, ax, shift_mode, deadline, task_ordering, c_value):
        keys = [
            parse_key_shift(shift_mode),
            parse_key_task_ordering(task_ordering, c_value),
            parse_key_factor(deadline)
        ]

        dict_temp = {}
        for key in keys:
            dict_temp[key] = True  # Set axe visible for key
            _add_to_mapping_list(ax, key, self.key_mapping)
        self.axes_keys[ax] = dict_temp

    def set_visibility(self, key, visibility):
        axes = self.key_mapping[key]

        for axe in axes:
            self.axes_keys[axe][key] = visibility

            if not visibility:
                axe.set_visible(False)
                continue

            all_keys_visible = True
            for _, is_visible in self.axes_keys[axe].items():
                if not is_visible:
                    all_keys_visible = False
                    break

            axe.set_visible(all_keys_visible)


def all_key(df_row):
    return f'{df_row.shift_mode}_{df_row.deadline_factor}_{str(df_row.c_value)}_{df_row.task_ordering}'


def _load_data(resources_path, relative_path, file_name):
    file = f'{resources_path}{relative_path}{file_name}'
    return pd.read_csv(file)

def load_data():
    #return load_small_workflows_results()  # 200
    return load_large_workflows_results()  # 1000


def load_large_workflows_results():  # 1000
    # relative_path = 'experiments/experiments_2025-05-01_16-15-28/'
    # file_name = 'report_experiments_2025-05-01_16-15-28_consolidated.csv'
    relative_path = 'experiments/experiments_large/'
    file_name = 'report_experiments_large_2025-05-01_16-15-28_and_2025-07-30_14-51-45_consolidated.csv'
    return _load_data(resources_path, relative_path, file_name)


def load_small_workflows_results():  # 200 tasks
    # relative_path = 'experiments/experiments_2025-05-04_02-54-15/'
    # file_name = 'report_experiments_2025-05-04_02-54-15_consolidated.csv'
    relative_path = 'experiments/experiments_small/'
    file_name = 'report_experiments_small_2025-05-04_02-54-15_and_2025-07-30_13-05-26_consolidated.csv'
    return _load_data(resources_path, relative_path, file_name)


def get_canvas_workflow_by_trace(df, color_and_marker_getter=lambda x:('red', 'o', DEFAULT_SHAPE_SIZE)):

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

            color, marker, size = color_and_marker_getter(row)
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


def plot_all_workflows(mapper, power_distribution, legend_mapper=None):

    n_rows = 2
    n_cols = 4
    assert n_rows * n_cols >= len(workflows), 'The number of subplots must be greater or equal to the number of workflows'

    df = load_data()
    if power_distribution is not None:
        df = df[df['power_distribution'] == power_distribution]

    fig, ax = plt.subplots(n_rows, n_cols, figsize=(40, 20))

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
    if legend_mapper is not None:
        handles = []
        for label, d in legend_mapper.items():
            color, shape, size = d
            handle = mlines.Line2D([], [], color=color, marker=shape, linestyle='None', markersize=10, label=label)
            handles.append(handle)
        plt.legend(handles=handles, loc='upper left', bbox_to_anchor=(-1.8, -0.5), ncol=3)

    #plt.legend(handles=handles, loc='upper center', bbox_to_anchor=(-1, -0.1), ncol=len(mapper.mapping))
    fig.tight_layout(pad=0)
    plt.savefig("teste.svg")
    plt.show()


def _apply_filters(filters, df):
    df_workflow = df[df['workflow'] == filters['workflow']]
    df_workflow = df_workflow[df_workflow['power_distribution'] == filters['power_distribution']]

    for filter_name, values in filters['multi-value-filters'].items():
        df_workflow = df_workflow[df_workflow[filter_name].isin(values)]

    return df_workflow

def plot_workflows_comparations(mapper, legend_mapper):

    name_map = {
        'genome': '                  1000Genome',
        'cycles': '            Cycles',
        'blast': '             BLAST',
        'srasearch': '                SRA Search',
    }

    filters_list = [
        {
            'workflow': 'genome',
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['none'],
                'task_ordering': ['energy']
            }
        },
        {
            'workflow': 'cycles',
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['none', 'left', 'right-left'],
                'c_value': [0.8],
                'deadline_factor': [8]
            }
        },
        {
            'workflow': 'blast',
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['none', 'right-left'],
                'task_ordering': ['runtime_ascending'],
                'deadline_factor': [8],
            }
        },
        {
            'workflow': 'srasearch',
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['none'],
                'deadline_factor': [8],
            }
        }
    ]

    n_rows = 2
    n_cols = 4

    large_workflows_df = load_large_workflows_results()
    small_workflows_df = load_small_workflows_results()

    fig = plt.figure(figsize=(40, 20))

    gs_main = GridSpec(n_rows, n_cols, figure=fig, hspace=0.2, wspace=0.2)


    letters_by_row_and_column = [
        ['a)', 'b)', 'c)', 'd)'],
        ['e)', 'f)', 'g)', 'h)']
    ]
    title_font_size = 20

    g_col = 0
    for filters in filters_list:
        large_workflows_df_filtered = _apply_filters(filters, large_workflows_df)

        gs_nested = GridSpecFromSubplotSpec(2, 2, subplot_spec=gs_main[0, g_col], hspace=0, wspace=0)
        axs = gs_nested.subplots(sharex='col', sharey='row')

        axs[0][0].set_title(
            name_map[filters['workflow']],
            fontsize=title_font_size
        )
        if g_col == 0:
            axs[0][0].set_ylabel('1.000 Tasks        ', fontsize=title_font_size)

        _add_subplot_of_traces(large_workflows_df_filtered, axs, color_and_marker_getter=mapper.get_color_and_marker,
                               controller=None, sub_plot_id=letters_by_row_and_column[0][g_col])


        small_workflows_df_filtered = _apply_filters(filters, small_workflows_df)

        gs_nested = GridSpecFromSubplotSpec(2, 2, subplot_spec=gs_main[1, g_col], hspace=0, wspace=0)
        axs = gs_nested.subplots(sharex='col', sharey='row')
        _add_subplot_of_traces(small_workflows_df_filtered, axs, color_and_marker_getter=mapper.get_color_and_marker,
                               controller=None, sub_plot_id=letters_by_row_and_column[1][g_col], show_legends=g_col==0)
        if g_col == 0:
            axs[0][0].set_ylabel('200 Tasks', fontsize=title_font_size)

        g_col +=1

    _create_legend_for_dynamic(fig, legend_mapper, None, frameon=False)

    fig.tight_layout(pad=0)
    plt.show()


def generate_report_file(output_figure, mapper, legend_mapper):

    name_map = {
        'blast': 'BLAST',
        'bwa': 'BWA',
        'cycles': 'Cycles',
        'genome': '1000Genome',
        'soykb': 'SoyKB',
        'srasearch': 'SRA Search',
        'montage': 'Montage',
        'seismology': 'Seismology',
    }

    filters_list = [
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {},
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'task_ordering': ['energy']
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'task_ordering': ['power']
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'task_ordering': ['runtime']
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'task_ordering': ['runtime_ascending']
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['none'],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['left'],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'shift_mode': ['right-left'],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'deadline_factor': [2],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'deadline_factor': [4],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'deadline_factor': [8],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'c_value': [0.0],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'c_value': [0.5],
            }
        },
        {
            'power_distribution': 'uniform',
            'multi-value-filters': {
                'c_value': [0.8],
            }
        },
    ]

    n_rows = 2
    n_cols = 4

    data_frames_loaders = [
        {
            'tasks': 1000,
            'loader': load_large_workflows_results
        },
        {
            'tasks': 200,
            'loader': load_small_workflows_results
        },
    ]


    letters_by_row_and_column = [
        ['a)', 'b)', 'c)', 'd)'],
        ['e)', 'f)', 'g)', 'h)']
    ]
    title_font_size = 20

    with PdfPages(output_figure) as pdf:

        for filters in filters_list:
            for data_frames_loader in data_frames_loaders:
                tasks = data_frames_loader['tasks']
                df_loader = data_frames_loader['loader']

                workflows_df = df_loader()

                fig = plt.figure(figsize=(40, 20))

                gs_main = GridSpec(n_rows, n_cols, figure=fig, hspace=0.2, wspace=0.2)

                g_row = 0
                g_col = 0

                for workflow in workflows:
                    filters['workflow'] = workflow
                    large_workflows_df_filtered = _apply_filters(filters, workflows_df)

                    gs_nested = GridSpecFromSubplotSpec(2, 2, subplot_spec=gs_main[g_row, g_col], hspace=0, wspace=0)
                    axs = gs_nested.subplots(sharex='col', sharey='row')

                    title = f'{name_map[workflow]} ({tasks} tasks)'
                    axs[0][0].set_title(title, fontsize=title_font_size)

                    _add_subplot_of_traces(large_workflows_df_filtered, axs, color_and_marker_getter=mapper.get_color_and_marker,
                                           controller=None, show_legends=(g_row == 1 and g_col == 0), sub_plot_id=letters_by_row_and_column[g_row][g_col])

                    g_col += 1
                    if g_col == 4:
                        g_col = 0
                        g_row = 1

                _create_legend_for_dynamic(fig, legend_mapper, None, frameon=False)

                fig.tight_layout(pad=0)
                pdf.savefig()
                plt.close()


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


def plot_big_legend(legend_mapping, title, symbols_per_row=36, marker_size_scale=10):

    # Extracted from: https://rowannicholls.github.io/python/graphs/symbols_linestyles_colours.html

    # Create axes
    fig, ax = plt.subplots()
    ax.set(
        title=title
    )

    for i, key in enumerate(legend_mapping):
        d = legend_mapping[key]
        color, maker, size = d
        x = int(np.floor(i / symbols_per_row) + 1)
        y = symbols_per_row - i % symbols_per_row + 0.1
        ax.plot([x], [y], marker=maker, color=color, markersize=size/marker_size_scale)
        x = np.floor(i / symbols_per_row) + 1.1
        y = symbols_per_row - i % symbols_per_row
        ax.annotate(key, [x, y], color=color)

    # Turn off axis ticks and tick labels
    plt.tick_params(axis='x', bottom=False, labelbottom=False)
    plt.tick_params(axis='y', left=False, labelleft=False)
    # Set the axis limits
    ax.set_xlim(0.9, 4)

    # Output
    plt.show()
    plt.close()

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
                    color, shape, size = d

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


def show_dynamic_plot(mapper, power_distribution, legend_mapper=None, controller=None):
    n_rows = 2
    n_cols = 4
    assert n_rows * n_cols >= len(workflows), 'The number of subplots must be greater or equal to the number of workflows'

    df = load_data()
    if power_distribution is not None:
        df = df[df['power_distribution'] == power_distribution]

    g_row = 0
    g_col = 0

    fig = plt.figure(figsize=(20, 10))
    gs_main = GridSpec(n_rows, n_cols, figure=fig, hspace=0.2, wspace=0.2)

    for workflow in workflows:
        df_workflow = df[df['workflow'] == workflow]

        gs_nested = GridSpecFromSubplotSpec(2, 2, subplot_spec=gs_main[g_row, g_col], hspace=0, wspace=0)
        axs = gs_nested.subplots(sharex='col', sharey='row')
        axs[0][0].set_title(workflow)
        _add_subplot_of_traces(df_workflow, axs, color_and_marker_getter=mapper.get_color_and_marker,
                               controller=controller)

        # Update next g_row and g_col
        g_col += 1
        if g_col % n_cols == 0:
            g_col = 0
            g_row += 1

    # Add legend
    if legend_mapper and controller:
        _create_legend_for_dynamic(fig, legend_mapper, controller)

    #fig.tight_layout(pad=100)
    plt.subplots_adjust(left=0.03, right=0.97, top=0.97, bottom=0.1)
    plt.show()


def _add_subplot_of_traces(df, axs, color_and_marker_getter=lambda x:('red', 'o', DEFAULT_SHAPE_SIZE), controller=None, sub_plot_id=None, show_legends=False):

    n_rows = 2
    n_cols = 2

    assert n_rows * n_rows >= len(traces), 'The number of subplots must be greater or equal to the number of traces'

    g_row = 0
    g_col = 0

    for trace in traces:

        df_workflow = df[df['energy_trace'] == trace]

        ax = axs[g_row, g_col]

        if sub_plot_id and g_row == g_col == 0:
            ax.text(0.04, 0.96, sub_plot_id,
                    transform=ax.transAxes,  # Use axes coordinates
                    fontsize=15,  # Font size of the title
                    verticalalignment='top',  # Align text to the top
                    horizontalalignment='left',  # Align text to the left
                    )

        ax.text(0.96, 0.96, trace,
                transform=ax.transAxes,  # Use axes coordinates
                fontsize=10,  # Font size of the title
                verticalalignment='top',  # Align text to the top
                horizontalalignment='right',  # Align text to the right
                bbox=dict(facecolor='white', alpha=0.6))



        if show_legends and g_row == 1 and g_col == 0:
            ax.set_xlabel('Makespan (minutes)')
            ax.set_ylabel('Brown energy (kJ)')

        for row in df_workflow.itertuples(index=True, name='Pandas'):
            makespan = row.AVERAGE_of_makespan / 60
            brown_energy = row.AVERAGE_of_brown_energy_used / 1000

            color, marker, size = color_and_marker_getter(row)
            scatter = ax.scatter(makespan, brown_energy, color=color, marker=marker, s=size)

            if controller is not None:
                controller.add(scatter, row.shift_mode, row.deadline_factor, row.task_ordering, row.c_value)

        # Update next g_row and g_col
        g_col += 1
        if g_col % n_cols == 0:
            g_col = 0
            g_row += 1


def _get_on_legend_click(legend, legend_mapper, controller, fig):
    key_visibility_mapping = {}
    for key in legend_mapper.keys():
        key_visibility_mapping[key] = True

    def on_legend_click(event):
        for legend_line, key in zip(legend.get_lines(), legend_mapper.keys()):
            if event.artist == legend_line:  # Check if the clicked artist is in the legend

                visible = not key_visibility_mapping[key]
                key_visibility_mapping[key] = visible
                legend_line.set_alpha(1.0 if visible else 0.2)  # Update legend appearance
                if controller:
                    controller.set_visibility(key, visible)

                fig.canvas.draw()  # Redraw canvas

    return on_legend_click


def _create_legend_for_dynamic(fig, legend_mapper, controller, pickradius=5, frameon=True):

    # Draw legend
    handles = []
    for label, d in legend_mapper.items():
        color, shape, size = d
        handle = mlines.Line2D([], [], color=color, marker=shape, linestyle='None', markersize=size / 10, label=label)
        handles.append(handle)
    legend = plt.legend(handles=handles, loc='upper left', bbox_to_anchor=(-7.1, -0.1), ncol=6, frameon=frameon)
    #legend = plt.legend(handles=handles, ncol=9)

    legend.set_draggable(True)

    for legend_line in legend.get_lines():
        legend_line.set_picker(pickradius)

    on_legend_click = _get_on_legend_click(legend, legend_mapper, controller, fig)

    # Connect the event to the figure
    fig.canvas.mpl_connect('pick_event', on_legend_click)


if __name__ == '__main__':

    # v2
    # plot_all_workflows(deadline_factor_map)
    # plot_all_workflows(c_values_map)
    # plot_all_workflows(shift_modes_map)
    # plot_all_workflows(task_ordering_map)

    # v3
    #plot_all_workflows(best_map, 'uniform')
    #plot_all_workflows(task_ordering_map, 'inverted_exponential')
    #plot_legend_only(shift_modes_map)
    #analyze_top_solutions()


    # v4
    #plot_big_legend(legend_lines, 'Scheduling Parameters', symbols_per_row=6, marker_size_scale=5)
    #plot_all_workflows(all_map, 'uniform')
    #plot_all_workflows(all_map, 'gaussian')
    #plot_all_workflows(all_map, 'inverted_exponential')

    #show_dynamic_plot(all_map, 'uniform', legend_lines, AxeVisibilityController())

    #plot_workflows_comparations(all_map, legend_lines)

    generate_report_file('./../../../results/experiment-1-figures.pdf', all_map, legend_lines)
