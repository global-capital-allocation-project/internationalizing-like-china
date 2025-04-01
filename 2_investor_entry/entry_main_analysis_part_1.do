******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/entry_main_analysis_part_1.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/investor_entry"
cap mkdir "${gcap_data}/output/investor_entry/temp"

******************************************************************************************************
* Crosswalk/auxiliary files from Other Sources:
******************************************************************************************************

* This table is a Factset product available on Factset DCS:
* Obtaining unique NAICS for every factset_entity_id
use "${gcap_data}/input/factset/ent_entity_naics_rank.dta", clear
keep if rank == 1
bys factset_entity_id: gen Nval = _N
assert Nval == 1
drop Nval
cap mkdir  "${gcap_data}/output/factset/"
save "${gcap_data}/output/factset/ent_entity_naics_rank_unique.dta", replace

******************************************************************************************************
* Obtaining Factset IDs for merging and getting more information: 
******************************************************************************************************

* 0. (Intermediary Step): manual search using "${gcap_data}/output/investor_entry/accession_all.xlsx"
* Output: "${gcap_data}/input/miscellaneous/factset_api_search.dta"

* 1. Merge info from Factset API
use "$gcap_data/output/investor_entry/accession_all.dta", clear
qui mmerge chinesename using "${gcap_data}/input/miscellaneous/factset_api_search.dta"
drop google _merge
keep chinesename englishname bondconnect bc_accession	cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii rqfii_accession Identifier  Name manual	Identifier_man	Name_man google 
destring bondconnect  cibm_commercial	cibm_central qfii rqfii, replace
order chinesename	englishname  bondconnect bc_accession cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii rqfii_accession Identifier	 manual  Identifier_man Name_man google
replace Identifier = "" if Identifier =="-"
replace Identifier = Identifier_man if Identifier ==""
drop Identifier_man	Name_man 
save "${gcap_data}/output/investor_entry/accession_all_fsid.dta", replace

* 2. Merge ISO_Code and SIC codes
use "${gcap_data}/output/investor_entry/accession_all_fsid.dta", clear
rename Identifier factset_entity_id
merge m:1 factset_entity_id using "${gcap_data}/input/factset/ent_entity_coverage.dta"
drop if _merge == 2
drop _merge
gen cou_missing=1 if iso_country == ""
replace cou_missing=0 if iso_country != ""
keep chinesename englishname  bondconnect bc_accession cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii rqfii_accession factset_entity_id	 manual google  primary_sic_code	industry_code	sector_code	iso_country
unique factset_entity_id
merge m:1 factset_entity_id using "${gcap_data}/output/factset/ent_entity_naics_rank_unique.dta"
drop if _merge == 2
keep chinesename englishname  bondconnect bc_accession cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii rqfii_accession factset_entity_id	 manual  primary_sic_code	industry_code	sector_code	iso_country  naics_code	rank  google 
order chinesename englishname  bondconnect bc_accession cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii rqfii_accession factset_entity_id	 manual  primary_sic_code	industry_code	sector_code	iso_country  naics_code	rank  google 
unique factset_entity_id
di _N
save "${gcap_data}/output/investor_entry/accession_all_fs_match.dta", replace
export excel using "${gcap_data}/output/investor_entry/accession_all_fs_match.xlsx", firstrow(variables) replace

* 2.5 (Intermediary Step): manual search country and NAICS for some: using "${gcap_data}/output/investor_entry/accession_all_fs_match.xlsx"
* Output: "${gcap_data}/input/miscellaneous/google_search_cou_naics.dta"

cap log close

