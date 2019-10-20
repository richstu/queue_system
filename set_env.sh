#!/bin/bash
export JB_QUEUE_SYSTEM_DIR=$(dirname $(readlink -e "$BASH_SOURCE"))
export PATH=$JB_QUEUE_SYSTEM_DIR/bin::$PATH
export PYTHONPATH=$JB_QUEUE_SYSTEM_DIR/libs:$PYTHONPATH
