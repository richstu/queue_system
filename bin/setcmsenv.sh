#!/bin/bash

# TODO: Change this by setting environment
WORK_DIR=$PWD
# Folders depended on user
CMSSW=/net/top/homes/jbkim/analysis/CMSSW
RELEASE=CMSSW_10_2_11_patch1

# Setup environment
. /cvmfs/cms.cern.ch/cmsset_default.sh
cd $CMSSW/$RELEASE/src
eval `scramv1 runtime -sh`
cd $WORK_DIR

echo [Info] CMSSW was set to $CMSSW/$RELEASE
echo [Info] WORK_DIR was set to $WORK_DIR

eval $@

##COMMAND="${@:2:$#-1}"
## Split and run commands
#IFS=';' read -ra COMMANDS <<< "$@"
#for COMMAND in "${COMMANDS[@]}"; do
#  echo $COMMAND
#  $COMMAND
#done
