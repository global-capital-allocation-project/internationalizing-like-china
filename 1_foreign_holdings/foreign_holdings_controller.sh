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
source "${2}/rmb_replication/master_shell_modules.sh" python

##############################################
# SUBMITS JOBS
##############################################

sleep 2m

if [ "${1}" = "translate_shch" ]; then
    umask 007
    python "${2}/rmb_replication/1_foreign_holdings/${1}.py" ${2}
else
     umask 007
     stata-mp -b "${2}/rmb_replication/1_foreign_holdings/${1}.do" ${2}
     rm -f "${2}/rmb_replication/${1}.log"

fi 

# FINISHED
echo "Finished Step "${1}
exit