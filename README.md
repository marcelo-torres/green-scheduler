# Green Scheduler

This Python project provides:
- Bounded-Boundary Search (BBS): A novel scheduling algorithm designed to maximize the use of renewable (green) energy within flexible deadlines.
- Green TaskFlow Scheduler: An enhanced, renewable energy-aware version of the TaskFlow scheduling algorithm [<reference>].
- Scheduling Simulator: A customizable simulator that models task execution on multi-core machines powered by both green and brown energy sources.
- Visualization Tools: Step-by-step visualization of task scheduling to help analyze algorithm behavior and energy usage.

## About BBS
Scientific workflows require significant computational power, resulting in considerable energy consumption and carbon
emissions. Renewable (green) energy sources are an alternative to minimize environmental impact. Solar energy, though 
intermittent, creates temporal energy heterogeneity that can be leveraged to minimize non-renewable (brown) energy 
usage. However, the intermittency may lead to task delay, increasing the workflow's finish time (makespan), 
a key user concern.

The BBS algorithm minimizes brown energy usage and makespan, under a deadline provided by the user. The deadline provide
slack time, which the BBS algorithm can use to delay tasks and use more green energy.

## Parameters:

The behavior of the BBS algorithm can be customized using the following parameters:

### Task Sort Strategies (task_sort)
Determines the priority queue for scheduling tasks. The task queue is not topologically sorted, so boundary constraints 
must ensure precedence preservation.

1) TASK_SORT_ENERGY - Energy (Runtime * Power)
2) TASK_SORT_POWER - Power required to run task
3) TASK_SORT_SPT - Task runtime ascending
4) TASK_SORT_LPT - Task runtime descending

### Boundary Strategies (boundary_strategy)

Defines how the algorithm computes constant boundaries (start/end windows) for each task.

1) BOUNDARY_SINGLE - Single machine with enough cores to run all tasks in parallel
2) BOUNDARY_DEFAULT - Estimate boundaries with limited cores
3) BOUNDARY_LPT_PATH - Same as strategy BOUNDARY_LPT_PATH, but sort predecessors and successors by LPT
4) BOUNDARY_LPT_FULL - Use LPT algorithm to create two schedules with predecessors and successors of a task. The length
of schedule is the boundary size.

### Variable Boundary Width (c)

A value c ∈ [0, 1) controls how much the constant boundary can expand to create variable boundaries for each task. 
A smaller c reduces slack; a larger c allows more scheduling flexibility.

### Shift Strategies (shift_mode)

The previous procedures may produce schedules with unnecessary idle time between tasks. Such idle times increase the 
workflow makespan without reducing brown energy usage. Shift procedures attempt to reduce makespan or brown energy 
usage after initial scheduling.

1) SHIFT_MODE_LEFT - Iterate over tasks by topological sort and reschedule tasks as early as possible, but saving the same 
or more brown energy. 
2) SHIFT_MODE_RIGHT_LEFT - Iterate over tasks by inverse topological sort and reschedule tasks as late as possible to minimize 
brown energy usage. Then, the shift-left is applied to reduce makespan.
3) SHIFT_MODE_NONE - No shift strategy is applied.

## Example

The figure below shows the scheduling of a sample workflow using BBS with the following configuration:
```
c = 0.5  
task_sort = TASK_SORT_ENERGY  
boundary_strategy = BOUNDARY_DEFAULT  
shift_mode = SHIFT_MODE_LEFT  
```

The workflow requires 1,380J and at least 60 seconds to execute. The schedule found by the algorithm uses 25J of 
brown energy and has a makespan of 89 seconds. This demonstrates how BBS leverages green energy availability while
remaining deadline-aware.

![Workflow](resources/figures/workflow.png?raw=true "Workflow")
![BBS Schedule](resources/figures/workflow_scheduling.png?raw=true "BBS Schedule")

## Repository Structure

```
resources
├── experiments
├── photovolta
├── results
└── wfcommons
    ├── real_traces
    └── synthetic

src
├── data
├── experiments
├── paper
├── scheduling
│   ├── algorithms
│   │   ├── bounded_boundary_search
│   │   │   ├── boundaries
│   │   │   ├── drawer
│   │   │   └── shift
│   │   ├── lpt
│   │   └── task_flow
│   ├── drawer
│   ├── energy
│   ├── model
│   └── util
└── util
```

## Running

### Run in docker container

1) Create the container
```shell
git pull https://github.com/marcelo-torres/green-scheduler.git
docker run -dit --name green-scheduler-experiment -v $PWD:/green-scheduler python:3.10 bash
```

2) Install dependencies
```shell
pip3 install pipreqs
pip3 install wfcommons
pip3 install Cython
pip3 install pyredblack
python3 -m pip install -r requirements.txt
```

3) Prepare the environment and run
```shell
export PYTHONPATH=/green-scheduler
cd /green_scheduler/src/experiments/main
python3 run_experiments.py
```

Docker useful commands
```shell
docker attach green-scheduler-experiment
```
```shell
^P^Q  // Escape sequence to detach container
```
```shell
docker logs green-scheduler-experiment (To view the historical logs)
```
```shell 
docker stop green-scheduler-experiment
```
```shell
docker rm green-scheduler-experiment
```


### Useful informations

Save requirements
```shell
pip3 freeze > requirements.txt
```


Activate venv
```shell
export PYTHONPATH=/home/marcelo_torres/workspace/green-scheduler/
virtualenv --python=python3.10 .venv
source .venv/bin/activate
```

Matplot issue in PyCharm 2023:
https://stackoverflow.com/questions/71798863/how-to-change-the-default-backend-in-matplotlib-from-qtagg-to-qt5agg-in-pych


