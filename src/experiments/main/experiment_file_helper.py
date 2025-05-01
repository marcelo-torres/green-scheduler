import csv
import os


def get_experiment_id(dt):
    return dt.strftime("experiments_%Y-%m-%d_%H-%M-%S")


def write_reports_to_csv(reports, headers, file_full_path):

    with open(file_full_path, 'a') as csvfile:
        csvwriter = csv.writer(csvfile)

        for report in reports:
            row = []
            for header in headers:
                row.append(
                    report[header]
                )
            csvwriter.writerow(row)


def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def create_csv_file(resources_path, start_time, headers):
    experiment_id = get_experiment_id(start_time)
    experiments_reports_path = resources_path + f'/experiments/{experiment_id}'
    file_full_path = f'{experiments_reports_path}/report_{experiment_id}.csv'

    create_dir(experiments_reports_path)
    with open(file_full_path, 'x') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)

    return file_full_path