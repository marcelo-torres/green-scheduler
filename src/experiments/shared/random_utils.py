import random


class RandomProvider:

    def __init__(self, seed, min_task_power, max_task_power):
        random.seed(seed)
        self.min_task_power = min_task_power
        self.max_task_power = max_task_power

    def random_uniform(self):
        return random.uniform(self.min_task_power, self.max_task_power)

    def random_gauss(self):
        mu = 3  # Mean
        sigma = 0.9  # Standard deviation

        value = random.gauss(mu, sigma)
        while value < self.min_task_power or value > self.max_task_power:
            value = random.gauss(mu, sigma)
        return value

    def random_expovariate(self):
        lambd = 1
        value = random.expovariate(lambd)
        while value < self.min_task_power or value > self.max_task_power:
            value = random.expovariate(lambd)
        return value

    def random_expovariate_inverse(self):
        return self.max_task_power - self.random_expovariate()