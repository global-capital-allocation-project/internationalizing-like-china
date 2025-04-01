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
partition_xml_read_unzip_x_large="maggiori,gsb"
time_xml_read_unzip_x_large="0-168:00:00"
ntasks_xml_read_unzip_x_large=7
mem_xml_read_unzip_x_large="64000"

partition_xml_read_unzip_large="maggiori,normal,gsb"
time_xml_read_unzip_large="0-48:00:00"
ntasks_xml_read_unzip_large=7
mem_xml_read_unzip_large="64000"

partition_xml_read_unzip_small="maggiori,normal,gsb"
time_xml_read_unzip_small="0-10:00:00"
ntasks_xml_read_unzip_small=7
mem_xml_read_unzip_small="64000"

partition_xml_read_convert_x_large="maggiori,gsb"
time_xml_read_convert_x_large="0-168:00:00"
ntasks_xml_read_convert_x_large=7
mem_xml_read_convert_x_large="64000"

partition_xml_read_convert_large="maggiori,gsb"
time_xml_read_convert_large="0-60:00:00"
ntasks_xml_read_convert_large=7
mem_xml_read_convert_large="64000"

partition_xml_read_convert_small="maggiori,normal,gsb"
time_xml_read_convert_small="0-60:00:00"
ntasks_xml_read_convert_small=7
mem_xml_read_convert_small="64000"

partition_Morningstar_Mapping_Build="${system_part}"
time_Morningstar_Mapping_Build="0-01:00:00"
ntasks_Morningstar_Mapping_Build=2
mem_Morningstar_Mapping_Build="20000"

partition_ER_Data_Build="${system_part}"
time_ER_Data_Build="0-00:30:00"
ntasks_ER_Data_Build=2
mem_ER_Data_Build="20000"

partition_PortfolioSummary_Build="${system_part}"
time_PortfolioSummary_Build="0-5:00:00"
ntasks_PortfolioSummary_Build=28
mem_PortfolioSummary_Build="41500"

partition_HoldingDetail_Build_1="${system_part}"
time_HoldingDetail_Build_1="0-5:00:00"
ntasks_HoldingDetail_Build_1=8
mem_HoldingDetail_Build_1="200000"

partition_HoldingDetail_Build_2_small="${system_part}"
time_HoldingDetail_Build_2_small="0-8:00:00"
ntasks_HoldingDetail_Build_2_small=28
mem_HoldingDetail_Build_2_small="41500"

partition_HoldingDetail_Build_2_large="${system_part}"
time_HoldingDetail_Build_2_large="0-48:00:00"
ntasks_HoldingDetail_Build_2_large=28
mem_HoldingDetail_Build_2_large="256000"

partition_HoldingDetail_Build_3="${system_part}"
time_HoldingDetail_Build_3="0-8:00:00"
ntasks_HoldingDetail_Build_3=8
mem_HoldingDetail_Build_3="700000"

partition_Refine_Parse_Externalid_large="${system_part}"
time_Refine_Parse_Externalid_large="0-8:00:00"
ntasks_Refine_Parse_Externalid_large=8
mem_Refine_Parse_Externalid_large="350000"

partition_externalid_prebloomberg="${system_part}"
time_externalid_prebloomberg="0-12:00:00"
ntasks_externalid_prebloomberg=8
mem_externalid_prebloomberg="500000"

partition_Figi_API="maggiori"
time_Figi_API="0-168:00:00"
ntasks_Figi_API=8
mem_Figi_API="500000"

############################################################################################################################
# DEFINE ARRAYS DEPENDING ON YEARS OF BUILD TO BE RUN
############################################################################################################################

# Hard coded year setting based on files sizes and data structure changes.
# These variables do not affect which years are run by the build.
switch_year=2002
switch_year_p1=$(( ${switch_year} + 1 ))
xml_end_year=2020
xl_year_start=2017

# NOTE: Files in monthly_new contain data going back to 2003, so need to run them for any end year from 2003 on.
# NOTE: For end years before 2003, only need to run sections that process historical data for those years.
# NOTE: After holding detail step, simply need to run jobs from start year to finish year.

# Set variable for whether to process monthly_new files
if ((start_year >= 2003 || end_year >= 2003)); then
    run_monthly=true
else
    run_monthly=false
fi
# Set variable for whether to process extra large historical files (those for 2017-2020)
if ((start_year <= 2020 && end_year >= 2017)); then
    run_x_large=true
else
    run_x_large=false
fi
# Set variable for whether to process large historical files (those from 2003 to 2016)
if ((start_year <= 2016 && end_year >= 2003)); then
    run_large=true
else
    run_large=false
fi
# Set variable for whether to process small historical files (those from 1986 to 2002)
if ((start_year <= switch_year)); then
    run_small=true
else
    run_small=false
fi
# Set variable for whether to process large HoldingDetail files (those from 2003 to 2016)
if ((end_year >= switch_year_p1)); then
    hd_run_large=true
else
    hd_run_large=false
fi
# Set variable for whether to process small HoldingDetail files (those from 1986 to 2002)
if ((start_year <= switch_year)); then
    hd_run_small=true
else
    hd_run_small=false
fi

# Define arrays for monthly_new data processing jobs
# Next line counts the raw folders in monthly_new (the relevent folders are in month_YYYY format)
month_n=$(( $(ls -d $gcap_data/input/morningstar/monthly_new/*_20*/ | wc -l) ))
# Set an array for the monthly_new xml_read step jobs
array_month="1-${month_n}"
# Set an array for the HoldingDetail_Build_1 step
array_after2020_from3="3-$(( ${month_n} + 3 ))"
# Set an array for the HoldingDetail_Build_Clean step
array_after2020="1-$(( ${month_n} + 3 ))"

# Define x_large jobs arrays for xml_read steps
# Get max of xl_year_start and start year
x_large_start=$((xl_year_start > start_year ? xl_year_start : start_year))
# Get min of xml_end_year and end year
x_large_end=$((xml_end_year < end_year ? xml_end_year : end_year))
# Define x_large file array
array_xml_read_x_large="${x_large_start}-${x_large_end}"

# Define large arrays for different jobs
# Get max of switch year plus 1 and start year
large_start=$((switch_year+1 > start_year ? switch_year+1 : start_year))
# Get min of xl_year_start minus 1 and end year
large_end=$((xl_year_start-1 < end_year ? xl_year_start-1 : end_year))
# Define large file array for xml_read_unzip jobs
array_xml_read_large="${large_start}-${large_end}"
# Define large file array for HoldingDetail jobs depending on end year
if [ "$end_year" -ge 2020 ]; then
    array_large_HD="${array_after2020},${large_start}-2019"
else
    array_large_HD="${large_start}-${end_year}"
fi
if [ "$start_year" -ge 2020 ] ; then
    array_large_HD="${array_after2020}"
fi
# Define large array for External ID jobs
array_large="${large_start}-${end_year}"

# Define small arrays for different jobs
# Define small array depending on end year
if [ "$end_year" -ge "$switch_year_p1" ]; then
    array_small="${start_year}-${switch_year}"
else
    array_small="${start_year}-${end_year}"
fi
# Set arrays for xml_read_ jobs that combine multiple arrays depending on what needs to be run
if [ "$run_monthly" = true ] && [ "$run_small" = true ] ; then
    array_xml_read_small="${array_small},${array_month}"
elif [ "$run_small" = true ] ; then
    array_xml_read_small="${array_small}"
elif [ "$run_monthly" = true ] ; then
    array_xml_read_small="${array_month}"
fi

# Set array for HoldingDetail_Build_3
# Get max of xml_end_year and start year
HD_3_start=$((xml_end_year > start_year ? xml_end_year : start_year))
# Set array
array_HoldingDetail_Build_3="${HD_3_start}-${end_year}"

# Define array for all years to be used in later steps where raw data structure is irrelevant
array_all="${start_year}-${end_year}"

# Export key variables
export morningstar_code_path
export morningstar_data_path


############################################################################################################################
# SUBMIT XML READ STEP
############################################################################################################################

# This section submits jobs to unzip the raw files and convert the xml data to dta files
# xml_read_unzip jobs uncompress the raw zip files into xml files
# xml_read_convert jobs convert the xml files to dta files

# this is for running just some blocks
case ${job} in
"unzip" | "all")

# NOTE: this block deletes in progress build files for the relevant years for the xml_read_unzip and xml_read_convert steps
echo "Checking for and deleting temp folder for unzip and xml conversion steps for years ${start_year}-${end_year}."
for yr in $(seq ${start_year} ${end_year}) ; do
    for folder in sas xml xml_concat xml_nobom ; do
        if [ -d "${morningstar_data_path}/${folder}/*_${yr}" ]; then
            echo remove ${morningstar_data_path}/temp/${folder}/*_${yr}
        fi
        if [ -d "${morningstar_data_path}/${folder}/historical/${yr}" ]; then
            echo remove ${morningstar_data_path}/temp/${folder}/historical/${yr}
        fi
    done
done
sleep 1

# Submit unzip job for extra large files
if [ "$run_x_large" = true ] ; then
xml_read_unzip_x_large_ID=`sbatch \
             --partition=${partition_xml_read_unzip_x_large} ${user_account} --time=${time_xml_read_unzip_x_large} \
             --nodes=${nodes} --ntasks=${ntasks_xml_read_unzip_x_large} --job-name=xml_read_unzip_x_large \
             --output="${morningstar_erroroutput_path}/xml_read_unzip_x_large-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_unzip_x_large-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_unzip_x_large} \
             --array=${array_xml_read_x_large} \
             "${morningstar_code_path}/read_xml/unzip.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_unzip_x_large Job: "${xml_read_unzip_x_large_ID}
sleep 1

# Set dependency for other unzip jobs to start after this one has started (since this job is by far the longest)
depend_unzip="--depend=after:${xml_read_unzip_x_large_ID}"
fi

# Submit unzip job for large files
if [ "$run_large" = true ] ; then
xml_read_unzip_large_ID=`sbatch \
             --partition=${partition_xml_read_unzip_large} ${user_account} --time=${time_xml_read_unzip_large} \
             --nodes=${nodes} --ntasks=${ntasks_xml_read_unzip_large} --job-name=xml_read_unzip_large \
             --output="${morningstar_erroroutput_path}/xml_read_unzip_large-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_unzip_x_large-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_unzip_large} \
             --array=${array_xml_read_large} ${depend_unzip} \
             "${morningstar_code_path}/read_xml/unzip.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_unzip_large Job: "${xml_read_unzip_large_ID}
sleep 1
fi

# Submit unzip job for small files (this job always runs since you need either the small or monthly files for any year range)
xml_read_unzip_small_ID=`sbatch \
        --partition=${partition_xml_read_unzip_small} ${user_account} --time=${time_xml_read_unzip_small} \
        --nodes=${nodes} --ntasks=${ntasks_xml_read_unzip_small} --job-name=xml_read_unzip_small \
        --output="${morningstar_erroroutput_path}/xml_read_unzip_small-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_unzip_small-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_unzip_small} \
        --array="${array_xml_read_small}" ${depend_unzip} \
        "${morningstar_code_path}/read_xml/unzip.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_unzip_small Job: "${xml_read_unzip_small_ID}
sleep 1

# job separator
;;&
"convert" | "all")

# Submit convert job for extra large files
if [ "$run_x_large" = true ] ; then
xml_read_convert_x_large_ID=`sbatch \
         --partition=${partition_xml_read_convert_x_large} ${user_account} --time=${time_xml_read_convert_x_large} \
         --nodes=${nodes} --ntasks=${ntasks_xml_read_convert_x_large} --job-name=xml_read_convert_x_large \
		 --output="${morningstar_erroroutput_path}/xml_read_convert_x_large-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_convert_x_large-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_convert_x_large} \
		 --array=${array_xml_read_x_large} \
         "${morningstar_code_path}/read_xml/save_dta.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_convert_x_large Job: "${xml_read_convert_x_large_ID}
sleep 1
# --depend=afterok:${xml_read_unzip_x_large_ID} \

# Set dependency for other convert jobs to start after this one has started (since this job is by far the longest)
depend_convert=",after:${xml_read_unzip_x_large_ID}"
fi

# Submit convert job for large files
if [ "$run_large" = true ] ; then
xml_read_convert_large_ID=`sbatch \
             --partition=${partition_xml_read_convert_large} ${user_account} --time=${time_xml_read_convert_large} \
             --nodes=${nodes} --ntasks=${ntasks_xml_read_convert_large} --job-name=xml_read_convert_large \
             --output="${morningstar_erroroutput_path}/xml_read_convert_large-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_convert_large-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_convert_large} \
             --array=${array_xml_read_large} \
             "${morningstar_code_path}/read_xml/save_dta.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_convert_large Job: "${xml_read_convert_large_ID}
sleep 1
#  --depend=after:${xml_read_convert_x_large_ID} \
# --depend=afterok:${xml_read_unzip_large_ID}${depend_convert} \
fi

# Submit convert job for small files (this job always runs since you need either the small or monthly files for any year range)
xml_read_convert_small_ID=`sbatch \
        --partition=${partition_xml_read_convert_small} ${user_account} --time=${time_xml_read_convert_small} \
        --nodes=${nodes} --ntasks=${ntasks_xml_read_convert_small} --job-name=xml_read_convert_small \
        --output="${morningstar_erroroutput_path}/xml_read_convert_small-%A_%a.out" --error="${morningstar_erroroutput_path}/xml_read_convert_small-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_xml_read_convert_small} \
        --array="${array_xml_read_small}" \
        --depend=after:${xml_read_convert_x_large_ID} \
        "${morningstar_code_path}/read_xml/save_dta.sh" ${U} | awk '{print $NF}'`
echo "Submitted xml_read_convert_small Job: "${xml_read_convert_small_ID}
sleep 1
#
# --depend=afterok:${xml_read_unzip_small_ID}${depend_convert} \



############################################################################################################################
# SUBMIT MAPPING BUILD, ER BUILD, AND PORTFOLIO SUMMARY BUILD
############################################################################################################################

# These jobs build the mapping files, the exchange rate data file, and the portfolio summary files

# job separator
;;&
"prep" | "all" | "after_convert")

# Morningstar Mapping Build: Build accompanying metadata (especially fund-level information) from Morningstar
Morningstar_Mapping_Build_ID=`sbatch \
         --partition=${partition_Morningstar_Mapping_Build} ${user_account} --time=${time_Morningstar_Mapping_Build} \
         --nodes=${nodes} --ntasks=${ntasks_Morningstar_Mapping_Build} --job-name=Morningstar_Mapping_Build \
		 --output="${morningstar_erroroutput_path}/Morningstar_Mapping_Build-%A_%a.out" --error="${morningstar_erroroutput_path}/Morningstar_Mapping_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Morningstar_Mapping_Build} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Morningstar_Mapping_Build ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Morningstar_Mapping_Build Job: "${Morningstar_Mapping_Build_ID}
sleep 1

# ER Data: Generate exchange rate data from IFS
ER_Data_Build_ID=`sbatch \
         --partition=${partition_ER_Data_Build} ${user_account} --time=${time_ER_Data_Build} \
         --nodes=${nodes} --ntasks=${ntasks_ER_Data_Build} --job-name=ER_Data_Build \
		 --output="${morningstar_erroroutput_path}/ER_Data_Build-%A_%a.out" --error="${morningstar_erroroutput_path}/ER_Data_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_ER_Data_Build} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} ER_Data_Build ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted ER_Data_Build Job: "${ER_Data_Build_ID}
sleep 1

# PortfolioSummary Build: Generate a clean dataset with portfolio summary data
# Set dependency variables based on build years
depend="--depend=afterok:${Morningstar_Mapping_Build_ID}:${xml_read_convert_small_ID}"
if [ "$run_x_large" = true ] ; then depend="${depend}:${xml_read_convert_x_large_ID}" ; fi
if [ "$run_large" = true ] ; then depend="${depend}:${xml_read_convert_large_ID}" ; fi
if [ "${job}" = "after_convert" ] || [ "${job}" = "prep" ]; then depend="--depend=afterok:${Morningstar_Mapping_Build_ID}" ; fi 
# Submit job
PortfolioSummary_Build_ID=`sbatch \
         --partition=${partition_PortfolioSummary_Build} ${user_account} --time=${time_PortfolioSummary_Build} \
         --nodes=${nodes} --ntasks=${ntasks_PortfolioSummary_Build} --job-name=PortfolioSummary_Build \
         --array=${array_all} \
		 --output="${morningstar_erroroutput_path}/PortfolioSummary_Build-%A_%a.out" --error="${morningstar_erroroutput_path}/PortfolioSummary_Build-%A_%a.err" \
         --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_PortfolioSummary_Build} \
         ${depend} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} PortfolioSummary_Build ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted PortfolioSummary_Build Job: "${PortfolioSummary_Build_ID}
sleep 1


############################################################################################################################
# SUBMIT HOLDING DETAIL BUILD
############################################################################################################################

# This section creates the main holding detail files by combining the historical and monthly holding data

# job separator
;;&
"holding_detail" | "all" | "after_convert")

# HoldingDetail Raw Stage: Generate raw monthly files for years from 2020
#### In this stage, the raw 2020/2021 monthly files, which include obs from 2003 to 2020/2021, will be generated and these files are used later to generate clean HoldingDetail files
depend="--depend=afterok:${xml_read_convert_small_ID}"
if [ "$run_x_large" = true ] ; then depend="${depend}:${xml_read_convert_x_large_ID}" ; fi
if [ "$run_large" = true ] ; then depend="${depend}:${xml_read_convert_large_ID}" ; fi
if [ "${job}" = "after_convert" ] || [ "${job}" = "holding_detail" ]; then depend="" ; fi  
# Submit job
if [ "$end_year" -ge 2003 ]; then
HoldingDetail_Build_1_ID=`sbatch \
             --partition=${partition_HoldingDetail_Build_1} ${user_account} --time=${time_HoldingDetail_Build_1} \
             --nodes=${nodes} --ntasks=${ntasks_HoldingDetail_Build_1} --job-name=HoldingDetail_Build_1 \
             --array=${array_after2020_from3} \
             --output="${morningstar_erroroutput_path}/HoldingDetail_Build_1-%A_%a.out" --error="${morningstar_erroroutput_path}/HoldingDetail_Build_1-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_HoldingDetail_Build_1} \
             ${depend} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} HoldingDetail_Build_1 ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted HoldingDetail_Build_1 Job: "${HoldingDetail_Build_1_ID}
sleep 1
# --depend=afterok:${depend} \
fi

# HoldingDetail Clean Stage: Read in the HoldingDetail files, clean and append them, and then merge with API and FX data
# We pass an extra argument, the number of monthly subfolders, so that the correct number files are appended in HoldingDetail_2
if [ "$hd_run_large" = true ] ; then
HoldingDetail_Build_2_large_ID=`sbatch \
             --partition=${partition_HoldingDetail_Build_2_large} ${user_account} --time=${time_HoldingDetail_Build_2_large} \
             --nodes=${nodes} --ntasks=${ntasks_HoldingDetail_Build_2_large} --job-name=HoldingDetail_Build_2_large \
             --array="${array_large_HD}" \
             --output="${morningstar_erroroutput_path}/HoldingDetail_Build_2_large-%A_%a.out" --error="${morningstar_erroroutput_path}/HoldingDetail_Build_2_large-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_HoldingDetail_Build_2_large} \
             --depend=afterok:${HoldingDetail_Build_1_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} HoldingDetail_Build_2 ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted HoldingDetail_Build_2_large Job: "${HoldingDetail_Build_2_large_ID}
sleep 1
# --depend=afterok:${Morningstar_Mapping_Build_ID}:${ER_Data_Build_ID}:${HoldingDetail_Build_1_ID} \

depend_Refine_Parse_Externalid="${depend_Refine_Parse_Externalid}:${HoldingDetail_Build_2_large_ID}"
fi

# appends monthly files output from HoldingDetail_Build_2.do for the years 2020 on
if [[ end_year -ge 2020 ]]; then
HoldingDetail_Build_3_ID=`sbatch \
             --partition=${partition_HoldingDetail_Build_3} ${user_account} --time=${time_HoldingDetail_Build_3} \
             --nodes=${nodes} --ntasks=${ntasks_HoldingDetail_Build_3} --job-name=HoldingDetail_Build_3 \
             --array="${array_HoldingDetail_Build_3}" \
             --output="${morningstar_erroroutput_path}/HoldingDetail_Build_3-%A_%a.out" --error="${morningstar_erroroutput_path}/HoldingDetail_Build_3-%A_%a.err" \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_HoldingDetail_Build_3} \
             --depend=afterok:${HoldingDetail_Build_2_large_ID} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} HoldingDetail_Build_3 ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted HoldingDetail_Build_3 Job: "${HoldingDetail_Build_3_ID}
sleep 1
# --depend=afterok:${HoldingDetail_Build_2_large_ID} \

depend_Refine_Parse_Externalid="${depend_Refine_Parse_Externalid}:${HoldingDetail_Build_3_ID}"
fi


############################################################################################################################
# SUBMIT EXTERNAL ID STEPS
############################################################################################################################

# This section creates a list of relevent identifiers then queries the FIGI API for

# job separator
;;&
"externalid" | "all" | "after_convert")

depend_Refine_Parse_Externalid="--depend=afterok:${HoldingDetail_Build_3_ID}"
if [ "${job}" = "externalid" ]; then depend_Refine_Parse_Externalid="" ; fi 
# Refine Parse Externalid: Clean and parse the externalid field in the Morningstar holdings data; this will
# be used in conjunction with the OpenFIGI API in order to identify securities for which we are otherwise
# lacking identifiers
if [ "$hd_run_large" = true ] ; then
 Refine_Parse_Externalid_large_ID=`sbatch \
             --partition=${partition_Refine_Parse_Externalid_large} ${user_account} --time=${time_Refine_Parse_Externalid_large} \
             --nodes=${nodes} --ntasks=${ntasks_Refine_Parse_Externalid_large} --job-name=Refine_Parse_Externalid_large \
             --output="${morningstar_erroroutput_path}/Refine_Parse_Externalid_large-%A_%a.out" --error="${morningstar_erroroutput_path}/Refine_Parse_Externalid_large-%A_%a.err" \
             --array=${array_large} \
             --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Refine_Parse_Externalid_large} \
             ${depend_Refine_Parse_Externalid} \
             "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} Refine_Parse_Externalid ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted Refine_Parse_Externalid_large Job: "${Refine_Parse_Externalid_large_ID}
sleep 1
# --depend=afterok${depend_Refine_Parse_Externalid} \

depend_externalid_prebloomberg="${depend_externalid_prebloomberg}:${Refine_Parse_Externalid_large_ID}"
fi

# Externalid (Pre-Bloomberg Stage): Consolidate the list of externalids to be sent to OpenFIGI via API
#### This stage is used to process the externalid part before the manual step involving the Bloomberg step.
#### And this stage includes two parts: (1) a stata do file to create a externalid list; (2) a R script to obtain OpenFIGI data via API using the list created before

# submit externalid_prebloomberg job
externalid_prebloomberg_ID=`sbatch \
         --partition=${partition_externalid_prebloomberg} ${user_account} --time=${time_externalid_prebloomberg} \
         --nodes=${nodes} --ntasks=${ntasks_externalid_prebloomberg} --job-name=externalid_prebloomberg \
		 --output="${morningstar_erroroutput_path}/externalid_prebloomberg-%A_%a.out" --error="${morningstar_erroroutput_path}/externalid_prebloomberg-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_externalid_prebloomberg} \
         --depend=afterok${depend_externalid_prebloomberg} \
         "${morningstar_code_path}/Morningstar_Build_Controller.sh" ${U} externalid_prebloomberg ${start_year} ${end_year} | awk '{print $NF}'`
echo "Submitted externalid_prebloomberg Job: "${externalid_prebloomberg_ID}
sleep 1

# job separator
;;&
"all" | "after_externalid")

depend_figi_api="--depend=afterok:${externalid_prebloomberg_ID}"
if [ "${job}" = "after_externalid" ]; then depend_figi_api="" ; fi 

# submit figi R api job
Figi_API_ID=`sbatch \
         --partition=${partition_Figi_API} ${user_account} --time=${time_Figi_API} \
         --nodes=${nodes} --ntasks=${ntasks_Figi_API} --job-name=Figi_API \
		 --output="${morningstar_erroroutput_path}/Figi_API-%A_%a.out" --error="${morningstar_erroroutput_path}/Figi_API-%A_%a.err" \
		 --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_Figi_API} \
             ${depend_figi_api} \
         "${morningstar_code_path}/externalid/R_Controller.sh" ${U} Figi_API | awk '{print $NF}'`
echo "Submitted Figi_API R Job: "${Figi_API_ID}
sleep 1

;;
esac


echo "Job IDs for all jobs submitted:"
echo "$xml_read_unzip_x_large_ID $xml_read_unzip_large_ID $xml_read_unzip_small_ID $xml_read_convert_x_large_ID $xml_read_convert_large_ID $xml_read_convert_small_ID $Morningstar_Mapping_Build_ID $ER_Data_Build_ID $PortfolioSummary_Build_ID $HoldingDetail_Build_1_ID $HoldingDetail_Build_2_small_ID $HoldingDetail_Build_2_large_ID $HoldingDetail_Build_3_ID $Refine_Parse_Externalid_small_ID $Refine_Parse_Externalid_large_ID $externalid_prebloomberg_ID $Figi_API_ID"

############################################################################################################################
####Manual Break Involving Bloomberg
############################################################################################################################
