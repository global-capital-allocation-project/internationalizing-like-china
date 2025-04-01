#!/bin/bash

# INPUT ENVIRONMENT VARIABLES
echo "STEP="${1}

# OUTPUT ENVIRONMENT VARIABLES
echo "SLURM_JOB_ID="$SLURM_JOB_ID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR
echo "SLURM_ARRAY_TASK_ID="$SLURM_ARRAY_TASK_ID
echo "SLURM_ARRAY_JOB_ID"=$SLURM_ARRAY_JOB_ID
echo "SLURM_NTASKS"=$SLURM_NTASKS

# load shell profile
source "${2}/rmb_replication/master_shell_profile.sh"

# load matlab  modules
source "${2}/rmb_replication/master_shell_modules.sh" matlab

##############################################
# SUBMITS JOBS
##############################################

# RUN MATLAB
matlab -nodisplay < "${2}/rmb_replication/4_theory/master_file.m"
# FINISHED
echo "Finished Job"
exit