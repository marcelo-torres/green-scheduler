from src.scheduling.energy.find_min_brown_energy import find_min_brown_energy_start


def schedule_min_brown_energy_min_start(task, machines, schedule, deadline, boundary_calc, energy_usage_calc, ignore_v_boundary=False):
    return _schedule_in_min_brown_energy(task, machines, schedule, deadline, boundary_calc, energy_usage_calc, False, ignore_v_boundary=ignore_v_boundary)


def schedule_min_brown_energy_max_start(task, machines, schedule, deadline, boundary_calc, energy_usage_calc, ignore_v_boundary=False):
    return _schedule_in_min_brown_energy(task, machines, schedule, deadline, boundary_calc, energy_usage_calc, True, ignore_v_boundary=ignore_v_boundary)


def _schedule_in_min_brown_energy(task, machines, schedule, deadline, boundary_calc, energy_usage_calculator, max_start_mode, ignore_v_boundary=False):
    best_machine = None
    selected_start_time = None
    smallest_brown_energy = float('inf')

    # Calculate boundaries to avoid that a single task gets all slack time
    lcb, lvb, rcb, rvb = boundary_calc.calculate_boundaries(task, schedule)
    if ignore_v_boundary:
        lb = lcb
        rb = rcb
    else:
        lb = lcb + lvb
        rb = rcb + rvb

    # Schedule each task when it uses less brown energy as early as possible
    green_power_available = energy_usage_calculator.get_green_power_available()

    interval_available = (deadline - rb) - lb
    if interval_available < task.runtime:
        raise Exception(f'Not enough time to schedule task {task.id}! Task runtime: {task.runtime}; Interval available: {interval_available}')

    for machine in machines:
        for start, end in machine.search_intervals_to_schedule_task(task, lb, deadline - rb):
            
            start_time, brown_energy = find_min_brown_energy_start(task, start, end, green_power_available, max_start_mode=max_start_mode)

            if _is_better(brown_energy, smallest_brown_energy, start_time, selected_start_time, max_start_mode):
                best_machine = machine
                selected_start_time = start_time
                smallest_brown_energy = brown_energy

    if best_machine is None:
        raise Exception(f'No machine available to schedule task {task.id}!')

    schedule[task.id] = selected_start_time, best_machine.id
    best_machine.schedule_task(task, selected_start_time)
    energy_usage_calculator.add_scheduled_task(task, selected_start_time)

    return lcb, lvb, rcb, rvb


def _is_better(brown_energy, smallest_brown_energy, start_time, selected_start_time, max_start_mode):
    if brown_energy < smallest_brown_energy:
        return True

    if brown_energy == smallest_brown_energy:
        if max_start_mode:
            return start_time > selected_start_time
        else:
            return start_time < selected_start_time

    return False
