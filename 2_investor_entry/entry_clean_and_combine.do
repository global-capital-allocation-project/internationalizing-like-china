******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/entry_clean_and_combine.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/investor_entry"
cap mkdir "${gcap_data}/output/investor_entry/temp"

ssc install filelist
******************************************************************************************************
* Cleaning Members: Bond Connect
******************************************************************************************************

cap mkdir "${gcap_data}/investor_entry/temp"
cap rm "${gcap_data}/investor_entry/temp/temp.dta"
cap restore
qui filelist, dir("${gcap_data}/input/bond_connect/csv")
keep if substr(filename, -4, .)== ".csv"
levelsof filename, local(csvfiles)

clear 
foreach csvfile of local csvfiles {
preserve
qui insheet using "${gcap_data}/input/bond_connect/csv/`csvfile'",  names clear
drop v1 no
qui save "${gcap_data}/output/investor_entry/temp/temp.dta", replace
restore
append using "${gcap_data}/output/investor_entry/temp/temp.dta"
}
cap rm "${gcap_data}/output/investor_entry/temp/temp.dta"

* Manual Adjustments:
qui do "${gcap_data}/rmb_replication/2_investor_entry/entry_manual_name_adj.do"
save "${gcap_data}/output/investor_entry/temp/bond_connect_long.dta", replace

bysort chinesename : egen accession1=min(accession)
sort englishname  accession
keep if accession == accession1
drop accession1
gen bc_yr = floor(accession/100)
gen bc_mon = accession - floor(accession/100)*100
replace accession = ym(bc_yr,bc_mon)
drop bc_yr bc_mon
rename accession bc_accession
format bc_accession %tm 
duplicates drop 
sort  chinesename
quietly by chinesename:  gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup
save "${gcap_data}/output/investor_entry/bond_connect_accession.dta", replace

******************************************************************************************************
* Cleaning Members: CIBM
******************************************************************************************************

filelist, dir("${gcap_data}/input/cibm/csv")
keep if  substr(filename, -4, .)== ".csv"
levelsof filename, local(csvfiles)

clear 
foreach csvfile of local csvfiles {
preserve
insheet using "${gcap_data}/input/cibm/csv/`csvfile'",  names clear
drop v1 no
qui save "${gcap_data}/output/investor_entry/temp/temp.dta", replace
restore
append using "${gcap_data}/output/investor_entry/temp/temp.dta"
}
cap rm "${gcap_data}/output/investor_entry/temp/temp.dta"

* Manual Adjustments:
qui do "${gcap_data}/rmb_replication/2_investor_entry/entry_manual_name_adj.do"
save "${gcap_data}/output/investor_entry/temp/cibm_long.dta", replace

bysort chinesename : egen accession1=min(accession)
sort englishname  accession
keep if accession == accession1
drop accession1
gen cibm_yr = floor(accession/100)
gen cibm_mon = accession - floor(accession/100)*100
replace accession = ym(cibm_yr,cibm_mon)
drop cibm_yr cibm_mon
rename accession cibm_accession
format cibm_accession %tm 
sort  chinesename
quietly by chinesename:  gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup
save "${gcap_data}/output/investor_entry/cibm_accession.dta", replace

******************************************************************************************************
* Cleaning Members: QFII
******************************************************************************************************

qui import excel "${gcap_data}/input/qfii/QFII_participants.xlsx", clear
drop A
drop if _n == 1
rename (B C D E F)  (chinesename englishname area accession major_custodian)
drop area major_custodian
duplicates drop

* Manual Adjustments:
qui do "${gcap_data}/rmb_replication/2_investor_entry/entry_manual_name_adj.do"

replace accession = subinstr(accession, "/", "-",.)
gen qfii_accession = date(accession, "MDY") if strpos(accession,"-")
format  qfii_accession %td 
replace qfii_accession = date(accession, "DMY") if ~strpos(accession,"-")
format  qfii_accession %td 
drop accession
gen bondconnect =0
gen cibm_commercial= 0 
gen cibm_central = 0
gen qfii =1
duplicates drop
sort  chinesename
qui by chinesename: gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup

save "${gcap_data}/output/investor_entry/qfii_accession.dta", replace

******************************************************************************************************
* Cleaning Members: RQFII
******************************************************************************************************

qui import excel "${gcap_data}/input/rqfii/RQFII_participants.xlsx", clear
drop A
drop if _n == 1
rename (B C D E F)  (chinesename englishname area accession major_custodian)
drop area major_custodian
duplicates drop

* Manual Adjustments:
qui do "${gcap_data}/rmb_replication/2_investor_entry/entry_manual_name_adj.do"

replace accession = subinstr(accession, "/", "-",.)
gen rqfii_accession = date(accession, "MDY") if strpos(accession,"-")
format  rqfii_accession %td 
replace rqfii_accession = date(accession, "DMY") if ~strpos(accession,"-")
format  rqfii_accession %td 
drop accession
gen bondconnect =0
gen cibm_commercial= 0 
gen cibm_central = 0
gen rqfii =1
duplicates drop
sort  chinesename
qui by chinesename:  gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup

save "${gcap_data}/output/investor_entry/rqfii_accession.dta", replace

******************************************************************************************************
* Merging: Bond Connect, CIBM, QFII
******************************************************************************************************

use "${gcap_data}/output/investor_entry/bond_connect_accession.dta", clear
drop cibm_commercial cibm_central 
merge 1:1 chinesename using "${gcap_data}/output/investor_entry/cibm_accession.dta"
drop _merge 
merge 1:1 chinesename using "${gcap_data}/output/investor_entry/qfii_accession.dta"
drop _merge
merge 1:1 chinesename using "${gcap_data}/output/investor_entry/rqfii_accession.dta"
drop _merge
order chinesename	englishname	 bondconnect 	bc_accession	cibm_commercial	cibm_central	cibm_accession	qfii	qfii_accession rqfii	rqfii_accession
replace  bondconnect =0 if bondconnect ==.
replace cibm_commercial = 0 if cibm_commercial ==.
replace cibm_central =0 if cibm_central ==.
replace qfii = 0 if qfii ==.
replace rqfii = 0 if rqfii ==.

sort chinesename
di _N
save "${gcap_data}/output/investor_entry/accession_all.dta", replace
export excel using "${gcap_data}/output/investor_entry/accession_all.xlsx", firstrow(variables) replace

cap log close