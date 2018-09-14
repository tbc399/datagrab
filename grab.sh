#!/usr/bin/env bash

# change into the main datagrab dir
cd ~/sonicred/datagrab/

# activate the virtual environment
source ~/sonicred/datagrab/venv/bin/activate

# run grab
python grab.py config.json

# deactivate the virtual environment (just to be neat)
deactivate
