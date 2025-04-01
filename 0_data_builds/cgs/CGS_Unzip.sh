#!/bin/bash

gcap_data="${1}"

# unzip 2020 data
raw_2022="$gcap_data/input/cgs/nov2022"
raw_2018="$gcap_data/input/cgs/nov2018"
raw_2016="$gcap_data/input/cgs/dec2016"

unzip_2022="$gcap_data/input/cgs/unzip/nov2022"
unzip_2018="$gcap_data/input/cgs/unzip/nov2018"
unzip_2016="$gcap_data/input/cgs/unzip/dec2016"

mkdir -p "$gcap_data/input/cgs/unzip"
mkdir -p $unzip_2022
mkdir -p $unzip_2018
mkdir -p $unzip_2016

for zip_file in $raw_2022/*.zip; do
    unzip -q -n -j "$zip_file" -d $unzip_2022
done

for zip_file in $raw_2018/*.zip; do
    unzip -q -n -j "$zip_file" -d $unzip_2018
done

for zip_file in $raw_2016/*.zip; do
    unzip -q -n -j "$zip_file" -d $unzip_2016
done

gunzip -c $raw_2016/ALLMASTER_ISIN.PIP.gz > $unzip_2016/ALLMASTER_ISIN.PIP

find "$raw_2018" -type f ! -name "*.zip" ! -name "*.gz" -exec sh -c 'cp "$0" "$1/$(basename "$0")"' {} "$unzip_2018" \;
find "$raw_2022" -type f ! -name "*.zip" ! -name "*.gz" -exec sh -c 'cp "$0" "$1/$(basename "$0")"' {} "$unzip_2022" \;