# Green Scheduler

## Save requirements
pip3 freeze > requirements.txt

## Run in venv
export PYTHONPATH=/home/marcelo_torres/workspace/green-scheduler/
virtualenv --python=python3.10 .venv
source .venv/bin/activate

pip3 install pipreqs
pip3 install wfcommons
pip3 install Cython
pip3 install pyredblack

python3 -m pip install -r requirements.txt


Matplot issue in PyCharm 2023:
https://stackoverflow.com/questions/71798863/how-to-change-the-default-backend-in-matplotlib-from-qtagg-to-qt5agg-in-pych


## Run in docker container

1) Create the container
```shell
docker run -dit --name green-scheduler-experiment -v $PWD:/green-scheduler python:3.10 bash
```

2) Install dependencies
```shell
pip3 install pipreqs
pip3 install wfcommons
pip3 install Cython
pip3 install pyredblack
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
^P^Q  // Escape sequence to detach container
docker logs green-scheduler-experiment (To view the historical logs) 
docker stop green-scheduler-experiment
docker rm green-scheduler-experiment
```
