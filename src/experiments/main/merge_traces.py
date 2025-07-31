import pandas as pd

def _merge_traces(original_file, update_file, output_file):

    # Keys to match rows on
    match_keys = [
        "iteration", "workflow", "energy_trace", "shift_mode", "c_value",
        "deadline_factor", "deadline", "boundary_strategy", "task_ordering", "power_distribution"
    ]

    # Columns to update from 'update'
    update_fields = [
        "scheduling_hash", "min_makespan", "makespan", "workflow_stretch",
        "brown_energy_used", "green_energy_used", "total_energy",
        "green_energy_not_used", "max_active_tasks", "active_tasks_mean",
        "active_tasks_std", "number_of_tasks", "scheduling_violations"
    ]

    # Columns to merge by concatenation
    concat_fields = {
        "job_number": "job_number",
        "experiment": "experiment",
        "experiment_type": "experiment_type"
    }

    # Load CSV files
    original_df = pd.read_csv(original_file)
    update_df = pd.read_csv(update_file)

    # Create index for joining
    original_df['_merge_key'] = original_df[match_keys].astype(str).agg('|'.join, axis=1)
    update_df['_merge_key'] = update_df[match_keys].astype(str).agg('|'.join, axis=1)

    # Check how many rows will be affected
    affected_rows = original_df['_merge_key'].isin(update_df['_merge_key']).sum()

    if affected_rows != len(update_df):
        raise ValueError(
            f"Mismatch: update file has {len(update_df)} rows, "
            f"but only {affected_rows} rows found in original to update."
        )

    # Set merge key as index for fast alignment
    original_df.set_index('_merge_key', inplace=True)
    update_df.set_index('_merge_key', inplace=True)

    # Update specified fields
    for col in update_fields:
        original_df.loc[update_df.index, col] = update_df[col]

    # Merge concatenated fields
    for orig_col, col in concat_fields.items():
        original_df.loc[update_df.index, orig_col] = (
            original_df.loc[update_df.index, col].astype(str) + "_" + update_df[col].astype(str)
        )

    # Reset index and drop temporary key
    original_df.reset_index(drop=True, inplace=True)
    original_df.to_csv(output_file, index=False)

    print(f"Merged file saved as: {output_file}")

def _merge_small_workflows(experiments_path):
    original_file = experiments_path + 'experiments_2025-05-04_02-54-15/report_experiments_2025-05-04_02-54-15.csv'
    update_file = experiments_path + 'experiments_2025-07-30_13-05-26/report_experiments_2025-07-30_13-05-26_small_partial_t3_t4.csv'
    output_file = experiments_path + 'experiments_small/report_experiments_small_2025-05-04_02-54-15_and_2025-07-30_13-05-26.csv'
    _merge_traces(original_file, update_file, output_file)

def _merge_large_workflows(experiments_path):
    original_file = experiments_path + 'experiments_2025-05-01_16-15-28/report_experiments_2025-05-01_16-15-28.csv'
    update_file = experiments_path + 'experiments_2025-07-30_14-51-45/report_experiments_2025-07-30_14-51-45_large_partial_t3_t4.csv'
    output_file = experiments_path + 'experiments_large/report_experiments_large_2025-05-01_16-15-28_and_2025-07-30_14-51-45.csv'
    _merge_traces(original_file, update_file, output_file)



if __name__ == "__main__":
    experiments_path = '../../../resources/experiments/'
    #_merge_small_workflows(experiments_path)
    _merge_large_workflows(experiments_path)
