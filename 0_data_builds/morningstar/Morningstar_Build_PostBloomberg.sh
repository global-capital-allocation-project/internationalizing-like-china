#!/bin/bash

############################################################################################################################
# SETUP
############################################################################################################################

# run profile
source master_shell_profile.sh

# sanity-checking that gcap_data variables exist
if [[ -z "$gcap_data" ]]; then
    echo "Empty gcap_data variable: Exiting..."
    exit 1
fi

# Defined by the user
start_year=2014
end_year=2020

# Email notification setting
mailtype="ALL"

# job settings
nodes=1

# set user variables
U=${whoami}
echo "System: "${system}
echo "User: "${U}

# for this version second argument is job name
job=${2}

# basic variables
#system_part="maggiori,normal,owners,gsb"
user_account=""

# morningstar specific paths
morningstar_code_path="$gcap_data/rmb_replication/0_data_builds/morningstar"
morningstar_erroroutput_path="$gcap_data/rmb_replication/0_data_builds/morningstar/erroroutput"
mkdir -p "${morningstar_erroroutput_path}"
morningstar_data_path="$gcap_data/output/morningstar"
read_xml_data_path="$gcap_data/input/morningstar"

# job parameters
partition_externalid_postbloomberg="$system_part"
time_externalid_postbloomberg="0-2:00:00"
ntasks_externalid_postbloomberg=8
mem_externalid_postbloomberg="500000"

partition_Refine_Cusip_Fill_Isin_1="$system_part"
time_Refine_Cusip_Fill_Isin_1="0-5:00:00"
ntasks_Refine_Cusip_Fill_Isin_1=28
mem_Refine_Cusip_Fill_Isin_1="41500"

partition_Refine_Cusip_Fill_Isin_2="$system_part"
time_Refine_Cusip_Fill_Isin_2="0-5:00:00"
ntasks_Refine_Cusip_Fill_Isin_2=8
mem_Refine_Cusip_Fill_Isin_2="350000"

partition_externalid_make="$system_part"
time_externalid_make="0-40:00:00"
ntasks_externalid_make=8
mem_externalid_make="500000"

partition_Refine_Extid_Merge_1="$system_part"
time_Refine_Extid_Merge_1="0-8:00:00"
ntasks_Refine_Extid_Merge_1=28
mem_Refine_Extid_Merge_1="41500"

partition_Refine_Extid_Merge_2="$system_part"
time_Refine_Extid_Merge_2="0-8:00:00"
ntasks_Refine_Extid_Merge_2=8
mem_Refine_Extid_Merge_2="350000"

partition_Internal_Currency="$system_part"
time_Internal_Currency="0-20:00:00"
ntasks_Internal_Currency=8
mem_Internal_Currency="500000"

partition_Refine_Cusip_Merge_1="$system_part"
time_Refine_Cusip_Merge_1="0-8:00:00"
ntasks_Refine_Cusip_Merge_1=8
mem_Refine_Cusip_Merge_1="41500"

partition_Refine_Cusip_Merge_2="$system_part"
time_Refine_Cusip_Merge_2="0-8:00:00"
ntasks_Refine_Cusip_Merge_2=28
mem_Refine_Cusip_Merge_2="600000"

partition_Internal_Class="$system_part"
time_Internal_Class="0-8:00:00"
ntasks_Internal_Class=8
mem_Internal_Class="350000"

partition_Manual_Corrections="$system_part"
time_Manual_Corrections="0-8:00:00"
ntasks_Manual_Corrections=8
mem_Manual_Corrections="600000"

partition_Create_Final_Files_1="$system_part"
time_Create_Final_Files_1="0-12:00:00"
ntasks_Create_Final_Files_1=28
mem_Create_Final_Files_1="41500"

partition_Create_Final_Files_2="$system_part"
 time_Create_Final_Files_2="0-12:00:00"
ntasks_Create_Final_Files_2=8
mem_Create_Final_Files_2="350000"

partition_Final_Clean_for_Analysis="maggiori"
time_Final_Clean_for_Analysis="0-12:00:00"
ntasks_Final_Clean_for_Analysis=28
mem_Final_Clean_for_Analysis="350000"

partition_Clean_Temp="$system_part"
time_Clean_Temp="0-00:10:00"
ntasks_Clean_Temp=1
mem_Clean_Temp="25000"

############################################################################################################################
# DEFINE ARRAYS FOR JOBS 
############################################################################################################################

# Hard coded year setting based on files sizes and data structure changes.
# These variables do not affect which years are run by the build.
switch_year=2005

# Set variable for whether to process large HoldingDetail files (those from 2003 to 2016)
if ((end_year > switch_year)); then
    run_large=true
else
    run_large=false
fi
# Set variable for whether to process small HoldingDetail files (those from 1986 to 2002)
if ((start_year <= switch_year)); then
    run_small=true
else
    run_small=false
fi

# Define small array for jobs
small_end=$((switch_year > end_year ? end_year : switch_year))
array_small="${start_year}-${small_end}"
# Define large array for jobs
large_start=$((switch_year+1 > start_year ? switch_year+1 : start_year))
array_large="${large_start}-${end_year}"

############################################################################################################################
# DEFINE ARRAYS FOR JOBS
############################################################################################################################


# count total number of years
n_years=$(($end_year - $start_year + 1))
# count of years in first split interval (1986-2005)
n_switch_years1=$(($switch_year - $start_year + 1))
# count of years in second split interval (2006-2020)
n_switch_years2=$(($end_year - $switch_year))

# count number of NonUS, 1986-2005
n_NonUS1=$(( $n_switch_years1 * 2 - 1))
# count start of NonUS, 2006-2020
n_NonUS2=$(($n_NonUS1 + 1))
# count number of halfs years, 1986-2020/end of NonUS, 1986-2020 (since we manually check 1986 h1)
n_halfs=$(( ($n_years) * 2 - 1))

#71 start of US, 1986-2005
n_US0=$(($n_halfs + 2))
#109 end of US, 1986-2005
n_US1=$(($n_US0 + $n_NonUS1 - 1))
#110 start of US, 2006-2020
n_US2=$(($n_US1 + 1))
#139 end of US, 2006-2020
n_US3=$(($n_halfs * 2 + 1))


# Export path variables
echo "Code path = "${morningstar_code_path}
echo "Data path = "${morningstar_data_path}
export morningstar_code_path
export morningstar_data_path

############################################################################################################################
# SUBMIT FRIST STEPS
############################################################################################################################

# this version runs a single jobs
case ${job} in
"first_batches" | "all" )

# Externalid (Post-Bloomberg Stage)
externalid_postbloomberg_ID=`sbatch \
         --partition=${partition_externalid_postbloomberg} ${user_account} --time=${time_externalid_postbloomberg} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_postbloomberg} --job-name=externalid_postbloomberg \
		 --output="${morningstar_erroroutput_path}/externalid_postbloomberg-%A_%a.out" --error="${morningstar_erroroutput_path}/externalid_postbloomberg-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_externalid_postbloomberg} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} externalid_postbloomberg ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted externalid_postbloomberg Job: "${externalid_postbloomberg_ID}
sleep 1

# Refine Cusip Fill Isin
depend_externalid_collect_extid_master=""
if [ "$run_small" = true ] ; then
    Refine_Cusip_Fill_Isin_1_ID=`sbatch \
             --partition=${partition_Refine_Cusip_Fill_Isin_1} ${user_account} --time=${time_Refine_Cusip_Fill_Isin_1} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Cusip_Fill_Isin_1} --job-name=Refine_Cusip_Fill_Isin_1 \
             --output="${morningstar_erroroutput_path}/Refine_Cusip_Fill_Isin_1-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Cusip_Fill_Isin_1-%A_%a.err" \
             --array=${array_small} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Cusip_Fill_Isin_1} \
             --depend=afterok:${externalid_postbloomberg_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Cusip_Fill_Isin ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Cusip_Fill_Isin_1 Job: "${Refine_Cusip_Fill_Isin_1_ID}
    sleep 1

    depend_externalid_collect_extid_master=":${Refine_Cusip_Fill_Isin_1_ID}"
fi

if [ "$run_large" = true ] ; then
    Refine_Cusip_Fill_Isin_2_ID=`sbatch \
             --partition=${partition_Refine_Cusip_Fill_Isin_2} ${user_account} --time=${time_Refine_Cusip_Fill_Isin_2} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Cusip_Fill_Isin_2} --job-name=Refine_Cusip_Fill_Isin_2 \
             --output="${morningstar_erroroutput_path}/Refine_Cusip_Fill_Isin_2-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Cusip_Fill_Isin_2-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Cusip_Fill_Isin_2} \
             --depend=afterok:${externalid_postbloomberg_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Cusip_Fill_Isin ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Cusip_Fill_Isin_2 Job: "${Refine_Cusip_Fill_Isin_2_ID}
    sleep 1

    depend_externalid_collect_extid_master="${depend_externalid_collect_extid_master}:${Refine_Cusip_Fill_Isin_2_ID}"
fi

############################################################################################################################
# SUBMIT SECOND STEPS
############################################################################################################################

# job separator
;;&
"second_batches" | "all" )

# set up dependcy if necessary 
# Set variable for whether to process small HoldingDetail files (those from 1986 to 2002)
if [ "$job" = "second_batches" ] ; then
    depend=""
else
    depend="--depend=afterok${depend_externalid_collect_extid_master}"
fi

# Externalid Make
#### This step generates an internal flatfile which has all security-level details for each externalid in the Morningstar holdings data.
externalid_collect_extid_master_ID=`sbatch \
         --partition=${partition_externalid_make} ${user_account} --time=${time_externalid_make} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_make} --job-name=externalid_collect_extid_master \
		 --output="${morningstar_erroroutput_path}/externalid_collect_extid_master-%A_%a.out" --error="${morningstar_erroroutput_path}/externalid_collect_extid_master-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_externalid_make} \
         ${depend} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} externalid_collect_extid_master ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted externalid_collect_extid_master Job: "${externalid_collect_extid_master_ID}
sleep 1

make_externalid_master_ID=`sbatch \
         --partition=${partition_externalid_make} ${user_account} --time=${time_externalid_make} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_make} --job-name=make_externalid_master \
		 --output="${morningstar_erroroutput_path}/make_externalid_master-%A_%a.out" --error="${morningstar_erroroutput_path}/make_externalid_master-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_externalid_make} \
		 --depend=afterok:${externalid_collect_extid_master_ID} \
         "${morningstar_code_path}/externalid/R_Controller.sh" ${U} make_externalid_master | awk '{print $NF}'`
echo "Submitted make_externalid_master Job: "${make_externalid_master_ID}
sleep 1

externalid_make_extid_csvtodta_ID=`sbatch \
         --partition=${partition_externalid_make} ${user_account} --time=${time_externalid_make} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_make} --job-name=externalid_make_extid_csvtodta \
		 --output="${morningstar_erroroutput_path}/externalid_make_extid_csvtodta-%A_%a.out" --error="${morningstar_erroroutput_path}/externalid_make_extid_csvtodta-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_externalid_make} \
		 --depend=afterok:${make_externalid_master_ID} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} externalid_make_extid_csvtodta ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted externalid_make_extid_csvtodta Job: "${externalid_make_extid_csvtodta_ID}
sleep 1

# Refine Extid Merge
depend_Refine_Extid_Merge=""
if [ "$run_small" = true ] ; then
    Refine_Extid_Merge_1_ID=`sbatch \
             --partition=${partition_Refine_Extid_Merge_1} ${user_account} --time=${time_Refine_Extid_Merge_1} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Extid_Merge_1} --job-name=Refine_Extid_Merge_1 \
             --output="${morningstar_erroroutput_path}/Refine_Extid_Merge_1-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Extid_Merge_1-%A_%a.err" \
             --array=${array_small} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Extid_Merge_1} \
             --depend=afterok:${externalid_make_extid_csvtodta_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Extid_Merge ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Extid_Merge_1 Job: "${Refine_Extid_Merge_1_ID}
    sleep 1

    depend_Refine_Extid_Merge=":${Refine_Extid_Merge_1_ID}"
fi
if [ "$run_large" = true ] ; then
    Refine_Extid_Merge_2_ID=`sbatch \
             --partition=${partition_Refine_Extid_Merge_2} ${user_account} --time=${time_Refine_Extid_Merge_2} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Extid_Merge_2} --job-name=Refine_Extid_Merge_2 \
             --output="${morningstar_erroroutput_path}/Refine_Extid_Merge_2-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Extid_Merge_2-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Extid_Merge_2} \
             --depend=afterok:${externalid_make_extid_csvtodta_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Extid_Merge ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Extid_Merge_2 Job: "${Refine_Extid_Merge_2_ID}
    sleep 1

    depend_Refine_Extid_Merge="${depend_Refine_Extid_Merge}:${Refine_Extid_Merge_2_ID}"
fi

# Internal Currency
Internal_Currency_ID=`sbatch \
         --partition=${partition_Internal_Currency} ${user_account} --time=${time_Internal_Currency} \
         --nodes=${nodes} --ntasks=${ntasks_Internal_Currency} --job-name=Internal_Currency \
		 --output="${morningstar_erroroutput_path}/Internal_Currency-%A_%a.out" --error="${morningstar_erroroutput_path}/Internal_Currency-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Internal_Currency} \
		 --depend=afterok${depend_Refine_Extid_Merge} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Internal_Currency ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Internal_Currency Job: "${Internal_Currency_ID}
sleep 1

# Refine Cusip Merge
depend_Refine_Cusip_Merge=""
if [ "$run_small" = true ] ; then
    Refine_Cusip_Merge_1_ID=`sbatch \
             --partition=${partition_Refine_Cusip_Merge_1} ${user_account} --time=${time_Refine_Cusip_Merge_1} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Cusip_Merge_1} --job-name=Refine_Cusip_Merge_1 \
             --output="${morningstar_erroroutput_path}/Refine_Cusip_Merge_1-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Cusip_Merge_1-%A_%a.err" \
             --array=${array_small} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Cusip_Merge_1} \
             --depend=afterok:${Internal_Currency_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Cusip_Merge ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Cusip_Merge_1 Job: "${Refine_Cusip_Merge_1_ID}
    sleep 1

    depend_Refine_Cusip_Merge=":${Refine_Cusip_Merge_1_ID}"
fi
if [ "$run_large" = true ] ; then
    Refine_Cusip_Merge_2_ID=`sbatch \
             --partition=${partition_Refine_Cusip_Merge_2} ${user_account} --time=${time_Refine_Cusip_Merge_2} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Cusip_Merge_2} --job-name=Refine_Cusip_Merge_2 \
             --output="${morningstar_erroroutput_path}/Refine_Cusip_Merge_2-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Cusip_Merge_2-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Cusip_Merge_2} \
             --depend=afterok:${Internal_Currency_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Cusip_Merge ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Refine_Cusip_Merge_2 Job: "${Refine_Cusip_Merge_2_ID}
    sleep 1

    depend_Refine_Cusip_Merge="${depend_Refine_Cusip_Merge}:${Refine_Cusip_Merge_2_ID}"
fi

# Internal Class
#### This step finds the modal typecode assigned to each fund in the Morningstar data.
Internal_Class_ID=`sbatch \
         --partition=${partition_Internal_Class} ${user_account} --time=${time_Internal_Class} \
         --nodes=${nodes} --ntasks=${ntasks_Internal_Class} --job-name=Internal_Class \
		 --output="${morningstar_erroroutput_path}/Internal_Class-%A_%a.out" --error="${morningstar_erroroutput_path}/Internal_Class-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Internal_Class} \
		 --depend=afterok${depend_Refine_Cusip_Merge} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Internal_Class ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Internal_Class Job: "${Internal_Class_ID}
sleep 1


# Manual Corrections
#### This step cleans several extraordinarily large positions in the holding details data.
Manual_Corrections_ID=`sbatch \
         --partition=${partition_Manual_Corrections} ${user_account} --time=${time_Manual_Corrections} \
         --nodes=${nodes} --ntasks=${ntasks_Manual_Corrections} --job-name=Manual_Corrections \
		 --output="${morningstar_erroroutput_path}/Manual_Corrections-%A_%a.out" --error="${morningstar_erroroutput_path}/Manual_Corrections-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Manual_Corrections} \
         --depend=afterok${depend_Refine_Cusip_Merge} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Manual_Corrections ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Manual_Corrections Job: "${Manual_Corrections_ID}
sleep 1
#

############################################################################################################################
# SUBMIT JOBS TO CREATE MASTER FILES
############################################################################################################################

# job separator
;;&
"final" | "all" )

# set up dependcy if necessary 
# Set variable for whether to process small HoldingDetail files (those from 1986 to 2002)
if [ "$job" = "final" ] ; then
    depend=""
else
    depend="--depend=afterok:${Internal_Class_ID}:${Manual_Corrections_ID}"
fi

# Create Final Files

depend_Create_Final_Files=""
if [ "$run_small" = true ] ; then
    Create_Final_Files_1_ID=`sbatch \
             --partition=${partition_Create_Final_Files_1} ${user_account} --time=${time_Create_Final_Files_1} \
             --nodes=${nodes} --ntasks=${ntasks_Create_Final_Files_1} --job-name=Create_Final_Files_1 \
             --output="${morningstar_erroroutput_path}/Create_Final_Files_1-%A_%a.out" --error="${morningstar_erroroutput_path}/Create_Final_Files_1-%A_%a.err" \
             --array=${array_small} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Create_Final_Files_1} \
             $depend \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Create_Final_Files ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Create_Final_Files_1 Job: "${Create_Final_Files_1_ID}
    sleep 1

    depend_Create_Final_Files=":${Create_Final_Files_1_ID}"
fi
if [ "$run_large" = true ] ; then
    Create_Final_Files_2_ID=`sbatch \
             --partition=${partition_Create_Final_Files_2} ${user_account} --time=${time_Create_Final_Files_2} \
             --nodes=${nodes} --ntasks=${ntasks_Create_Final_Files_2} --job-name=Create_Final_Files_2 \
             --output="${morningstar_erroroutput_path}/Create_Final_Files_2-%A_%a.out" --error="${morningstar_erroroutput_path}/Create_Final_Files_2-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Create_Final_Files_2} \
             $depend \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Create_Final_Files ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Create_Final_Files_2 Job: "${Create_Final_Files_2_ID}
    sleep 1

    depend_Create_Final_Files="${depend_Create_Final_Files}:${Create_Final_Files_2_ID}"
fi

############################################################################################################################
# SUBMIT JOBS TO CREATE MASTER FILES
############################################################################################################################

# job separator
;;&
"hd_analysis" | "all" )

# set up dependcy if necessary 
# Set variable for whether to process small HoldingDetail files (those from 1986 to 2002)
if [ "$job" = "hd_analysis" ] ; then
    depend=""
else
    depend="--depend=afterok:${Create_Final_Files_1_ID}:${Create_Final_Files_2_ID}"
fi

depend_Final_Clean_for_Analysis=""

# Final Cleaning and Standardizations
     Final_Clean_for_Analysis_ID=`sbatch \
             --partition=${partition_Final_Clean_for_Analysis} ${user_account} --time=${time_Final_Clean_for_Analysis} \
             --nodes=${nodes} --ntasks=${ntasks_Final_Clean_for_Analysis} --job-name=Create_Final_Files_1 \
             --output="${morningstar_erroroutput_path}/Final_Clean_for_Analysis-%A_%a.out" --error="${morningstar_erroroutput_path}/Final_Clean_for_Analysis-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Final_Clean_for_Analysis} \
             $depend \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Final_Clean_for_Analysis ${start_year} ${end_year} | awk '{print $NF}'`
    echo "Submitted Final_Clean_for_Analysis Job: "${Final_Clean_for_Analysis_ID}
    sleep 1


############################################################################################################################
# SUBMIT CLEAN STEPS
############################################################################################################################

# job separator
;;&
"clean" | "all")
if [ "$job" = "all" ] ; then
    depend="--depend=afterok:${depend_Create_Final_Files}:${Create_Final_Files_2_ID}"
else
    depend=""
fi

# Clean Temp Files
JOB_Clean_Temp_ID=`sbatch \
         --partition=${partition_Clean_Temp} ${user_account} --time=${time_Clean_Temp} \
         --nodes=${nodes} --ntasks=${ntasks_Clean_Temp} --job-name=Clean_Temp \
		  --output=${morningstar_erroroutput_path}/Clean_Temp-%j.out --error=${morningstar_erroroutput_path}/Clean_Temp-%j.err \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
         $depend \
         --mem=${mem_Clean_Temp} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Clean_Temp ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Clean_Temp Job: "${JOB_Clean_Temp_ID}
sleep 1

;;
esac

echo "Job IDs for all jobs submitted:"
echo "$externalid_postbloomberg_ID $Refine_Cusip_Fill_Isin_1_ID $Refine_Cusip_Fill_Isin_2_ID $externalid_collect_extid_master_ID $make_externalid_master_ID $externalid_make_extid_csvtodta_ID $Refine_Extid_Merge_1_ID $Refine_Extid_Merge_2_ID $Internal_Currency_ID $Refine_Cusip_Merge_1_ID $Refine_Cusip_Merge_2_ID $Internal_Class_ID $Manual_Corrections_ID $Create_Final_Files_1_ID $Create_Final_Files_2_ID $JOB_Clean_Temp_ID"
