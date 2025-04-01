#!/bin/bash

# run master_shell_profile.sh
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_profile.sh"
# load modules
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_modules.sh" sas

# save code path
morningstar_code_path="$gcap_data/rmb_replication/0_data_builds/morningstar"
morningstar_data_path="$gcap_data/output/morningstar"
read_xml_data_path="$gcap_data/input/morningstar"

echo "STEP: Save XML to DTA"
echo ${morningstar_code_path}

foldertype=$2

############################################################################################################################
# CODE FOR HISTORICAL DATA
############################################################################################################################

# Code for historical data (with array variable starting from $start_year)
if [ "$SLURM_ARRAY_TASK_ID" -ge 1950 ]; then
    year=$SLURM_ARRAY_TASK_ID

    for zip_file in $(ls -1 ${read_xml_data_path}/historical/*$year*.{zip,7z})
    do
        
        echo "Iteration for $zip_file"
        
        zip_file=${zip_file##*/}
        zip_folder=${zip_file%.*}

        xml_path="${morningstar_data_path}/temp/xml_concat/historical"
        sas_path="${morningstar_data_path}/temp/sas/historical/$year/$zip_folder"
        dta_path="${read_xml_data_path}/historical/DTA/$year/$zip_folder"
        
        # Remove existing folders for output from this file if they exist
        if [ -d "${sas_path}" ]; then
            echo "    Folder for SAS output for $folder found. Emptying this folder."
            rm -rf "${sas_path}"
        fi 
        if [ -d "${dta_path}" ]; then
            echo "    Folder for dta output for $folder found. Emptying this folder."
            rm -rf "${dta_path}"
        fi 

        # Make folder for output from this file
        mkdir -p $dta_path
        mkdir -p $sas_path

        count=0
        for xml_file in $(find ${morningstar_data_path}/temp/xml_concat/historical/$year/$zip_folder/ -name "xml_*")
        do
        
            echo "    Iteration for $xml_file"
            
            count=`expr $count + 1`

            xml_file=${xml_file##*/}
            xml_name=${xml_file%.*}

            echo "        Converting $xml_file within historical/$year/$zip_folder folder..."

            if [[ ! -f $dta_path/PortfolioSummary_$count.dta || ! -f $dta_path/HoldingDetail_$count.dta ]]; then
                sas -sysparm "$xml_path/$year/$zip_folder/,$dta_path/,$xml_file,$count" \
                    -log     "$sas_path/$xml_name.log" \
                    ${morningstar_code_path}/read_xml/convert_xml_to_dta.sas 
            fi
            
            sleep 15

            filesize_1=$(stat -c %s $dta_path/PortfolioSummary_$count.dta)
            filesize_2=$(stat -c %s $dta_path/HoldingDetail_$count.dta)
            echo "        Original: PortfolioSummary size $filesize_1; HoldingDetail size $filesize_2"
            resubmit=0
            until [[ $filesize_1 != 0 && $filesize_2 != 0 ]]
            do
                sas -sysparm "$xml_path/$year/$zip_folder/,$dta_path/,$xml_file,$count" \
                    -log     "$sas_path/$xml_name.log" \
                    ${morningstar_code_path}/read_xml/convert_xml_to_dta.sas && break 
                    
                sleep 15

                resubmit=`expr $resubmit + 1`
                filesize_1=$(stat -c %s $dta_path/PortfolioSummary_$count.dta)
                filesize_2=$(stat -c %s $dta_path/HoldingDetail_$count.dta)
                echo "        Resubmit $resubmit: PortfolioSummary size $filesize_1; HoldingDetail size $filesize_2"
            done
            echo "        Finish converting $xml_file within historical/$year/$zip_folder folder."
            #rm -rf $xml_path/$year/$zip_folder/$xml_file
        done
    done
    
    echo "Completed conversion for ${year}."
fi


############################################################################################################################
# CODE FOR MONTHLY_NEW DATA
############################################################################################################################

# Code for monthly_new data (with array variable starting from 1)
if [ "$SLURM_ARRAY_TASK_ID" -lt 1950 ]; then

    # need to tranform task id into month year, starting with 1 as April_2020 where monthly data starts
    # set month, with first month being April 
    if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 1  ]; then month=April; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 2  ]; then month=May; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 3  ]; then month=June; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 4  ]; then month=July; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 5  ]; then month=August; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 6  ]; then month=September; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 7  ]; then month=October; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 8  ]; then month=November; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 9  ]; then month=December; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 10 ]; then month=January; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 11 ]; then month=February; fi
	if [ $(($SLURM_ARRAY_TASK_ID % 12)) -eq 0  ]; then month=March; fi
    # set year to cycle forward from 2020, with 10,22,... being new years
    year_i=$(( $(($SLURM_ARRAY_TASK_ID + 2)) / 12 ))
    year=$(( 2020 + $year_i))
    # set folder name
    folder=${month}_${year}

    xml_path="${morningstar_data_path}/temp/xml/$folder"
    dta_path="${read_xml_data_path}/monthly_new/DTA/$folder"
    sas_path="${morningstar_data_path}/temp/sas/$folder"
    
    echo "Array for $folder"

    # Remove existing folders for output from this file if they exist
    if [ -d "${sas_path}" ]; then
        echo "Folder for SAS output for $folder found. Emptying this folder."
        rm -rf "${sas_path}"
    fi 
    if [ -d "${dta_path}" ]; then
        echo "Folder for dta output for $folder found. Emptying this folder."
        rm -rf "${dta_path}"
    fi 

    # Make folder for output from this file
    mkdir -p "${sas_path}"
    mkdir -p "${dta_path}"

    for xml_file in $(find $xml_path/ -name "*.xml")  # ls cannot be used here since for some folders, there are too many files, which will cause error.
    do
    
        echo "Iteration for $xml_file"
        
        count=`expr $count + 1`

        xml_file=${xml_file##*/}
        xml_name=${xml_file%.*}
        xml_name=`echo $xml_name | cut -c 15-22`

        if [[ $xml_name == FO_CAN_F ]]; then
            xml_name="FO_CAN_F_M"
        fi
        if [[ $xml_name == FO_CAN_R ]]; then
            xml_name="FO_CAN_RBC_M"
        fi

        echo "    Converting $xml_file within $folder folder..."

        if [[ ! -f $dta_path/PortfolioSummary_$xml_name.dta || ! -f $dta_path/HoldingDetail_$xml_name.dta ]]; then
            sas -sysparm "$xml_path/,$dta_path/,$xml_file,$xml_name" \
                -log     "$sas_path/$xml_name.log" \
                -work    "$sas_path" \
                ${morningstar_code_path}/read_xml/convert_xml_to_dta.sas 
        fi
        
        sleep 15
        
        filesize_1=$(stat -c %s $dta_path/PortfolioSummary_$xml_name.dta)
        filesize_2=$(stat -c %s $dta_path/HoldingDetail_$xml_name.dta)
        echo "    Original: PortfolioSummary size $filesize_1; HoldingDetail size $filesize_2"
        resubmit=0
        until [[ $filesize_1 != 0 && $filesize_2 != 0 ]]
        do
            sas -sysparm "$xml_path/,$dta_path/,$xml_file,$xml_name" \
                -log     "$sas_path/$xml_name.log" \
                -work    "$sas_path" \
                ${morningstar_code_path}/read_xml/convert_xml_to_dta.sas 
                
            sleep 15

            resubmit=`expr $resubmit + 1`
            filesize_1=$(stat -c %s $dta_path/PortfolioSummary_$xml_name.dta)
            filesize_2=$(stat -c %s $dta_path/HoldingDetail_$xml_name.dta)
            echo "    Resubmit $resubmit: PortfolioSummary size $filesize_1; HoldingDetail size $filesize_2"
        done
        echo "    Finish converting $xml_name file within $folder folder."
        #rm -rf $xml_path/$xml_file

    done
    
    echo "Completed conversion for ${folder}."
fi
