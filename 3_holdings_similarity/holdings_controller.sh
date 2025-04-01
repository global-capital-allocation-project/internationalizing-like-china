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

# load modules
source "${2}/rmb_replication/master_shell_modules.sh" stata


##############################################
# SUBMITS JOBS
##############################################


sleep 2m

# check the code exists: without this check the file will try to run the code, will fail, but will not return an error
if ! [ -f "${2}/rmb_replication/3_holdings_similarity/${1}.do" ] 
then
    echo "No file found. Exiting..."
    exit 1
fi

# set default permissions for newly created files and directories
umask 007

# running the job
stata-mp -b "${2}/rmb_replication/3_holdings_similarity/${1}.do" ${SLURM_ARRAY_TASK_ID}
rm -f "${2}/rmb_replication/${1}.log"

# FINISHED
echo "Finished Step "${2}

exit


