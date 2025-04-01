#!/bin/bash

# run master_shell_profile.sh
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_profile.sh"
# load modules
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_modules.sh" p7zip
source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_modules.sh" python

echo "STEP: Unzip"

morningstar_data_path="$gcap_data/output/morningstar"
read_xml_data_path="$gcap_data/input/morningstar"

############################################################################################################################
# CODE FOR HISTORICAL DATA
############################################################################################################################

# Code for historical data (with array variable starting from $start_year)
if [ "$SLURM_ARRAY_TASK_ID" -ge 1950 ]; then

	xml_nobom_path="${morningstar_data_path}/temp/xml_nobom/historical"
	xml_concat_path="${morningstar_data_path}/temp/xml_concat/historical"
	mkdir -p $xml_nobom_path
	mkdir -p $xml_concat_path
	mkdir -p "$read_xml_data_path/historical/DTA"
	mkdir -p "$morningstar_data_path/temp/xml/historical"

	year=$SLURM_ARRAY_TASK_ID

	zip_path="${read_xml_data_path}/historical"
	xml_path="${morningstar_data_path}/temp/xml/historical"

	max=10000 #Concat 10000 files each time

	for zip_file in $(ls -1 ${zip_path}/*$year*.{zip,7z})
	do
        zip_file=${zip_file##*/}
        zip_folder=${zip_file%.*}
        mkdir -p $xml_path/$year/$zip_folder
        7za e -y -o$xml_path/$year/$zip_folder $zip_path/$zip_file

        # Concatenating Step
        total=0
        for xml_file in $(find $xml_path/$year/$zip_folder/ -name "*.xml")
        do
            total=`expr $total + 1`
        done

        num=`expr $total / $max + 1`

        mkdir -p $xml_nobom_path/$year/$zip_folder
        mkdir -p $xml_concat_path/$year/$zip_folder

        xml_count=0

        for concat_num in $( eval echo {1..$num} )
        do

            rm -f $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
            # start our output file:
            #   add header & <file> tags
            echo '<?xml version="1.0" encoding="utf-8"?>' > $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
            echo '<File>' >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

            echo "Processing XML files"

            for xml_file in $(find $xml_path/$year/$zip_folder -name "*.xml")
            do
                # strip off path, so we can direct appropriately
                xml_file=${xml_file##*/}
                xml_count=`expr $xml_count + 1`
                start=`expr $concat_num - 1`
                start=`expr $start \* $max + 1`
                end=`expr $concat_num \* $max`

                if [[ $xml_count -gt $end ]]; then
                    break
                fi

                if [[ $xml_count -ge $start && $xml_count -le $end ]]; then
                    # convert from UTF-8 to ascii (remove BOM)
                    iconv --from-code UTF-8 --to-code US-ASCII -c $xml_path/$year/$zip_folder/$xml_file > $xml_nobom_path/$year/$zip_folder/$xml_file

                    #   remove XML header and append to output file
                    #     doesn't matter if we grab this from XMLs with BOM or noBOM
                    perl  -pe 's/^.+?utf-8"\?>//' $xml_nobom_path/$year/$zip_folder/$xml_file >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
                    echo >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

                    # do some cleanup, so the # of files in the dir doesn't get too large
                    rm $xml_nobom_path/$year/$zip_folder/$xml_file
                    rm $xml_path/$year/$zip_folder/$xml_file
                fi
            done

            # add final </file> tag
            echo '</File>' >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

            echo "# of files in $folder/$year/$zip_folder concatenated: $xml_count"
        done

        ###If not all xml files merged, first remove the concat file not properly processed (i.e. the concat files with 1KB);
        ##then, merge remaining xml files to the concat file with the number following the largest number the current concat files have.
        ##The above two steps will repeat until all xml files merged

        result=$(find $xml_path/$year/$zip_folder/ -name "*.xml"| wc -l)

        until [[ $result == 0 ]]; do
            find $xml_concat_path/$year/$zip_folder/ -size 1k -name "xml_*" -delete

            count=0
            total=0

            for xml_file in $(find $xml_concat_path/$year/$zip_folder/ -name "xml_*")
            do
                count=`expr $count + 1`
            done

            for xml_file in $(find $xml_path/$year/$zip_folder/ -name "*.xml")
            do
                total=`expr $total + 1`
            done

            num=`expr $total / $max + 1 + $count`
            start=`expr $count + 1`

            for concat_num in $( eval echo {$start..$num} )
            do

                rm -f $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
                # start our output file:
                #   add header & <file> tags
                echo '<?xml version="1.0" encoding="utf-8"?>' > $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
                echo '<File>' >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

                echo "Processing XML files"

                for xml_file in $(find $xml_path/$year/$zip_folder -name "*.xml")
                do
                    # strip off path, so we can direct appropriately
                    xml_file=${xml_file##*/}
                    xml_count=`expr $xml_count + 1`
                    start=`expr $concat_num - 1`
                    start=`expr $start \* $max + 1`
                    end=`expr $concat_num \* $max`

                    if [[ $xml_count -gt $end ]]; then
                        break
                    fi

                    if [[ $xml_count -ge $start && $xml_count -le $end ]]; then
                        # convert from UTF-8 to ascii (remove BOM)
                        iconv --from-code UTF-8 --to-code US-ASCII -c $xml_path/$year/$zip_folder/$xml_file > $xml_nobom_path/$year/$zip_folder/$xml_file

                        #   remove XML header and append to output file
                        #     doesn't matter if we grab this from XMLs with BOM or noBOM
                        perl  -pe 's/^.+?utf-8"\?>//' $xml_nobom_path/$year/$zip_folder/$xml_file >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num
                        echo >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

                        # do some cleanup, so the # of files in the dir doesn't get too large
                        rm $xml_nobom_path/$year/$zip_folder/$xml_file
                        rm $xml_path/$year/$zip_folder/$xml_file
                    fi
                done

                # add final </file> tag
                echo '</File>' >> $xml_concat_path/$year/$zip_folder/xml_concat$concat_num

                echo "# of files in $folder/$year/$zip_folder concatenated: $xml_count"
            done

            result=$(find $xml_path/$year/$zip_folder/ -name "*.xml"| wc -l)

        done

	done

	echo "Finish unzipping files within historical/$year folder"

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

    # run unzip commands on folder
    zip_path="zip_path_$folder"
    xml_path="xml_path_$folder"
    dta_path="dta_path_$folder"
    eval $zip_path="$read_xml_data_path/monthly_new/$folder"
    eval $xml_path="$morningstar_data_path/temp/xml/$folder"
    eval $dta_path="$read_xml_data_path/monthly_new/DTA/$folder"
    mkdir -p "${!xml_path}"
    mkdir -p "${!dta_path}"

    zip_path="${read_xml_data_path}/monthly_new/$folder"
    xml_path="${morningstar_data_path}/temp/xml/$folder"

    7za e -y -o$xml_path $zip_path
    7za e -y -o$xml_path $xml_path

    find $xml_path/ -name "*.xml.zip" -exec rm -rf {} \; &
    find $xml_path/ -name "*.xml.gz" -exec rm -rf {} \; &
    find $xml_path/ -name "*.txt" -exec rm -rf {} \;
    echo "Finish unzipping files within $folder folder"

fi
