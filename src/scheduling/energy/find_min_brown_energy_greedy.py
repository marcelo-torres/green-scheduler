def find_min_brown_energy_greedy(task, lb, rb, deadline, calculator):

    start = lb

    start_min = start
    min_brown_energy_usage = float('inf')

    while start + task.runtime <= deadline - rb:

        calculator.add_scheduled_task(task, start)
        brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage()
        calculator.remove_scheduled_task(task, start)

        if brown_energy_used < min_brown_energy_usage:
            min_brown_energy_usage = brown_energy_used
            start_min = start
        start += 1
    return start_min