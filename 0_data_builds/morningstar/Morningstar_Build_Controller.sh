#!/bin/bash
# INPUT ENVIRONMENT VARIABLES
echo "USER="${1}
echo "STEP="${2}
echo "START_YEAR="${3}
echo "END_YEAR="${4}


# OUTPUT ENVIRONMENT VARIABLES
echo "SLURM_JOB_ID="$SLURM_JOB_ID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR
echo "SLURM_ARRAY_TASK_ID="$SLURM_ARRAY_TASK_ID
echo "SLURM_ARRAY_JOB_ID"=$SLURM_ARRAY_JOB_ID
echo "SLURM_NTASKS"=$SLURM_NTASKS

# run profile
source ~/master_shell_profile.sh

# load modules
source ~/master_shell_modules.sh stata

# RUN CALCULATIONS
umask 007
stata-mp -b "${gcap_data}/rmb_replication/0_data_builds/morningstar/Morningstar_Build.do" ${1} ${2} ${3} ${4} ${SLURM_ARRAY_TASK_ID}

# CHECK STATA LOGS FOR ERRORS
# THIS LOOP WILL EXIT WITH AN ERROR IF THE STATA LOG HAS AN ERROR CODE, SO SLURM WILL NOTIFY YOU IF STATA HAS PROBLEMS
echo "Checking Stata log for errors..."
if [ -z ${SLURM_ARRAY_TASK_ID+x} ]
then 
    if egrep --before-context=2 --max-count=1 "^r\([0-9]+\);$" "$gcap_data/output/morningstar/logs/${2}/${1}_${2}.log"
    then
        exit 1
    fi
else 
    if egrep --before-context=2 --max-count=1 "^r\([0-9]+\);$" "$gcap_data/output/morningstar/logs/${2}/${1}_${2}_Array_${SLURM_ARRAY_TASK_ID}.log"
    then
        exit 1
    fi
fi
echo "No errors found."

# FINISHED
echo "Finished Step "${2}
exit
