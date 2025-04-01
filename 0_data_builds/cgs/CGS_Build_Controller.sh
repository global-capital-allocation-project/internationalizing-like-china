#!/bin/bash

# INPUT ENVIRONMENT VARIABLES
echo "STEP="${1}
gcap_data=${2}

# OUTPUT ENVIRONMENT VARIABLES
echo "SLURM_JOB_ID="$SLURM_JOB_ID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR
echo "SLURM_ARRAY_TASK_ID="$SLURM_ARRAY_TASK_ID
echo "SLURM_ARRAY_JOB_ID"=$SLURM_ARRAY_JOB_ID

# run profile
source "${2}/rmb_replication/master_shell_profile.sh"

# load stata
source "${2}/{rmb_replication}/master_shell_modules.sh" stata

# RUN CALCULATIONS
umask 007
stata-mp -b "${rmb_replication}/0_data_builds/cgs/CGS_Build.do" ${1} ${2} ${SLURM_ARRAY_TASK_ID} 


# FINISHED
echo "Finished Step "${1}
exit
