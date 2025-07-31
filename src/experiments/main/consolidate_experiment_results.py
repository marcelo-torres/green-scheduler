# Columns to group by
columns_to_copy = [
    'energy_trace', 'power_distribution', 'workflow', 'c_value',
    'deadline_factor', 'shift_mode', 'task_ordering', 'iteration'
]

# Columns for statistics calculation
columns_to_calc = [
    'brown_energy_used', 'green_energy_used', 'makespan', 'workflow_stretch'
]


def consolidate_experiment_results(experiments_file):
    import pandas as pd

    df = pd.read_csv(experiments_file)
    result_df = df[columns_to_copy].copy()

    df_stats = df[columns_to_calc]
    result_df = pd.concat([result_df, df_stats], axis=1)

    # Group by all columns except iteration
    grouped = result_df.groupby([col for col in columns_to_copy if col != 'iteration'])

    # Calculate mean and std for columns_to_calc only
    aggregated = grouped[columns_to_calc].agg(['mean', 'std']).reset_index()

    # Flatten multi-level column names
    def aggregated_name(col_name, aggregation_operation):
        if aggregation_operation == 'mean':
            return f'AVERAGE_of_{col_name}'
        elif aggregation_operation == 'std':
            return f'STD_of_{col_name}'
        else:
            return f'{aggregation_operation}_{col_name}'

    aggregated.columns = [col[0] if col[1] == '' else aggregated_name(col[0], col[1]) for col in aggregated.columns]

    # Save the consolidated results
    output_file = experiments_file.replace('.csv', '_consolidated.csv')
    aggregated.to_csv(output_file, index=False, float_format='%.6f')


if __name__ == '__main__':

    #experiments_1000_file = '../../../resources/experiments/experiments_2025-05-01_16-15-28/report_experiments_2025-05-01_16-15-28.csv'
    #experiments_200_file = '../../../resources/experiments/experiments_2025-05-04_02-54-15/report_experiments_2025-05-04_02-54-15.csv'

    experiments_1000_file = '../../../resources/experiments/experiments_large/report_experiments_large_2025-05-01_16-15-28_and_2025-07-30_14-51-45.csv'
    experiments_200_file = '../../../resources/experiments/experiments_small/report_experiments_small_2025-05-04_02-54-15_and_2025-07-30_13-05-26.csv'

    consolidate_experiment_results(experiments_1000_file)
    #consolidate_experiment_results(experiments_200_file)
