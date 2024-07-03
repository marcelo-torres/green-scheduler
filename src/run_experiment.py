from src.scheduling.algorithms.highest_power_first.highest_power_first import schedule_graph
from src.scheduling.energy.energy_usage_calculator import EnergyUsageCalculator




def run(graph, deadline, green_power, interval_size, c, max_green_power):
    scheduling = schedule_graph(graph, deadline, green_power, interval_size, c=c, show='last', max_power=max_green_power)

    calculator = EnergyUsageCalculator(graph, green_power, interval_size)
    brown_energy_used, green_energy_not_used, total_energy = calculator.calculate_energy_usage_for_scheduling(scheduling)

    makespan = get_makespan(scheduling, graph)

if __name__ == '__main__':

    # Workflow
    # Energy Trace
    # deadline
    # c-value

    pass