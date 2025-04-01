#!/bin/bash
# INPUT ENVIRONMENT VARIABLES
echo "USER="${1}
echo "STEP="${2}

# OUTPUT ENVIRONMENT VARIABLES
echo "SLURM_JOB_ID="$SLURM_JOB_ID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURM_SUBMIT_DIR="$SLURM_SUBMIT_DIR
echo "SLURM_ARRAY_TASK_ID="$SLURM_ARRAY_TASK_ID
echo "SLURM_ARRAY_JOB_ID"=$SLURM_ARRAY_JOB_ID
echo "SLURM_NTASKS"=$SLURM_NTASKS


# run master_shell_profile.sh
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_profile.sh"

# load R module 
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_modules.sh" R
ml purge
ml R/3.5.1
ml system harfbuzz fribidi
ml cmake libgit2
ml openssl

# make error folder if it doesn't exist 
mkdir -p "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/output/morningstar/logs/${2}"

# RUN R SCRIPT
R CMD BATCH --no-save --no-restore '--args tempdir="/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/output/morningstar/temp/externalid" rawdir="/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/input/morningstar/externalid"' "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/0_data_builds/morningstar/externalid/${2}.R" "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/output/morningstar/logs/${2}/${1}_${2}.out"

# FINISHED
echo "Finished Step "${2}
exit
