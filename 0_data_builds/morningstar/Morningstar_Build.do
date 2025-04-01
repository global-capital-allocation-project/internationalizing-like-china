clear
set more off
set excelxlsxlargefile on

* Set year variables from arguments 
global gcap_data = "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication"
global firstyear = `3'
global lastyear = `4'
global lastyear_mf = `4'
di "firstyear = `3'"
di "lastyear = `4'"
di "lastyear_mf = `4'"

* log file
cap mkdir "$gcap_data/output/morningstar/logs/`2'"
cap log close 
if "`5'"!="" {
    log using "$gcap_data/output/morningstar/logs/`2'/`1'_`2'_Array_`5'.log", replace
}
else {
    log using "$gcap_data/output/morningstar/logs/`2'/`1'_`2'.log", replace
}

* Install all required packages from SSC
cap ssc install mmerge
cap ssc install fs
cap ssc install unique
cap ssc install ftools
cap ssc install kountry
cap ssc install nmissing
cap ssc install egenmore

* Install all required packages from the web
local github "https://raw.githubusercontent.com"
cap net install gtools.pkg, from(`github'/mcaceresb/stata-gtools/master/build/)

*** Macros to Define Paths*****************************************************
global morningstar_code_path "$gcap_data/rmb_replication/0_data_builds/morningstar"
global dir_mstar_raw "$gcap_data/input/morningstar"
global dir_mstarbuild "$gcap_data/output/morningstar"
global output "$dir_mstarbuild/output"
global logs "$dir_mstarbuild/logs"
global temp "$dir_mstarbuild/temp"

* make general folders
cap mkdir $dir_mstar_raw
cap mkdir $dir_mstarbuild
cap mkdir $output
cap mkdir $temp
cap mkdir $logs //

* make subfolder in logs and temp
foreach folder in externalid Internal mf_unwind {
	cap mkdir $logs/`folder'
	cap mkdir $temp/`folder'
}
* make subfolder in logs, temp, and output
foreach folder in Morningstar_Mapping_Build ER_Data PortfolioSummary HoldingDetail HD_for_analysis {
	cap mkdir $logs/`folder'
	cap mkdir $temp/`folder'
	cap mkdir $output/`folder'
}

*******************************************************************************

* externalid steps
if "`2'"=="externalid_make" {
	do $morningstar_code_path/externalid/collect_extid_master.do
	do $morningstar_code_path/externalid/make_extid_csvtodta.do
}
else if substr("`2'",1,10) =="externalid" {
	do $morningstar_code_path/externalid/`2'.do
}

* figi R step 
else if "`2'"=="Figi_API" {
    !source "/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/master_shell_modules.sh" R; R CMD BATCH --no-save --no-restore '--args tempdir="/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/output/morningstar/temp/externalid" rawdir="/oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/input/morningstar/externalid"' /oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/morningstar/externalid/Figi_API.R /oak/stanford/groups/maggiori/GCAP/data/cdms1_replication/rmb_replication/morningstar/logs/Figi_API/figi_api.out
}

* folder cleaning step
else if "`2'"=="Clean_Temp" {
	shell rm -rf "$temp"
}

* all other steps
else {
    do $morningstar_code_path/`2'.do `5'
}

cap log close
