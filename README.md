# Green Scheduler

This Python project provides:
- Bounded-Boundary Search (BBS): A novel scheduling algorithm designed to maximize the use of renewable (green) energy within flexible deadlines.
- Green TaskFlow Scheduler: An enhanced, renewable energy-aware version of the TaskFlow scheduling algorithm [<reference>].
- Scheduling Simulator: A customizable simulator that models task execution on multi-core machines powered by both green and brown energy sources.
- Visualization Tools: Step-by-step visualization of task scheduling to help analyze algorithm behavior and energy usage.

## How it works
Scientific workflows require significant computational power, resulting in considerable energy consumption and carbon
emissions. Renewable (green) energy sources are an alternative to minimize environmental impact. Solar energy, though 
intermittent, creates temporal energy heterogeneity that can be leveraged to minimize non-renewable (brown) energy 
usage. However, the intermittency may lead to task delay, increasing the workflow's finish time (makespan), 
a key user concern.

The BBS algorithm minimizes brown energy usage and makespan, under a deadline provided by the user.

Boundary

Shift strategy

Parameters:

![Workflow](resources/figures/workflow.png?raw=true "Workflow")
![BBS Schedule](resources/figures/workflow_scheduling.png?raw=true "BBS Schedule")

## Project structure

```shell
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


