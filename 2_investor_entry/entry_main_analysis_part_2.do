******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/entry_main_analysis_part_2.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/investor_entry"
cap mkdir "${gcap_data}/output/investor_entry/temp"

******************************************************************************************************
* Crosswalk/auxiliary files from Other Sources:
******************************************************************************************************

* NAICS Codes
qui import excel "${gcap_data}/input/naics/6-digit_2017_Codes.xlsx", sheet("2017_6-digit_industries") cellrange(A1:B1059) firstrow clear
cap drop if missing(NAICSCode)
rename (NAICSCode NAICSTitle) (naics_code	naics_title)
save "${gcap_data}/input/naics/naics_title.dta", replace

******************************************************************************************************
* Continue obtaining more info about investors using FactSet
******************************************************************************************************

* 3. Merge info from manual search
use "$gcap_data/output/investor_entry/accession_all_fs_match.dta", clear
qui mmerge chinesename using "${gcap_data}/input/miscellaneous/google_search_cou_naics.dta"
drop _merge
save "${gcap_data}/output/investor_entry/accession_all_fs_match_google.dta", replace
export excel using "${gcap_data}/output/investor_entry/accession_all_fs_match_google.xlsx", firstrow(variables) replace

* 4. Manual Corrections and NAICS description:
use  "${gcap_data}/output/investor_entry/accession_all_fs_match_google.dta", clear
destring bondconnect cibm_commercial cibm_central qfii rank	google_cou	google_naic naics_correction	fsid_correction, replace
replace google_cou = 0 if google_cou == .
replace google_naics = 0 if google_naics == .
replace naics_correction = 0 if naics_correction == .
replace fsid_correction= 0 if fsid_correction == .
replace iso_country = iso_country_gg if google_cou == 1|fsid_correction==1
replace naics_code = naics_code_gg  if google_naics == 1|naics_correction==1|fsid_correction==1
drop  iso_country_gg naics_code_gg naics_desc
merge m:1 naics_code using "${gcap_data}/input/naics/naics_title.dta"
drop if _merge ==2
drop _merge
rename bondconnect bc
save "${gcap_data}/output/investor_entry/accession_all_cou_naics.dta", replace

* 5. Additional Cleaning and Other Factset Info: 
replace factset=trim(factset)
mmerge factset_entity_id using "${gcap_data}/input/factset/entity_id_manual_fixes.dta"
drop if _merge==2
replace factset_e=updated_f if updated_f~=""
drop updated_f
*Merge in ultimate parent
qui mmerge factset_entity_id using "${gcap_data}/input/factset/ent_entity_structure.dta", ukeep(factset_ultimate)
drop if _merge==2
*Merge in ultimate parent country and name
qui mmerge factset_ultimate using "${gcap_data}/input/factset/sym_entity.dta", umatch(factset_entity_id) ukeep(iso_co entity_proper_name) uname(up_)
drop if _merge==2
gen dummy_fs=0
replace dummy_fs=1 if factset_entity_id~=""
gen dummy_upfs=0
replace dummy_upfs=1 if factset_ultimate_parent_entity_i~=""
order iso_co up_iso factset*
tab dumm*
drop _merge
kountry iso_co, from(iso2c) to(iso3c)
replace iso_co=_ISO
drop _ISO
kountry up_iso, from(iso2c) to(iso3c)
replace up_iso=_ISO
drop _ISO
gen bc_date=dofm(bc_accession)
gen cibm_date=dofm(cibm_accession)
gen qfii_date=qfii_accession
gen rqfii_date=rqfii_accession
format bc_date %td
format cibm_date %td
format qfii_date %td
format rqfii_date %td
order bc_date cibm_date qfii_date rqfii_date 
gen n=_n
replace factset_ultimate_parent_entity_i="xxx" if factset_ultimate_parent_entity_i==""
mmerge factset_ultimate_parent_entity_i using "${gcap_data}/input/factset/ent_entity_naics_rank.dta", umatch(factset_entity_id) uname(up_) 
drop if _merge==2
drop if up_rank~=1 & _merge==3
mmerge up_naics_code using "${gcap_data}/input/factset/naics6_map.dta", umatch(naics6_code) ukeep(naics6_desc) uname(up_)
drop if _merge==2
destring primary_sic_code, replace
destring industry_code, replace
mmerge primary_sic_code using "${gcap_data}/input/factset/sic_map.dta", umatch(sic_code)
drop if _merge==2
mmerge industry_code using "${gcap_data}/input/factset/factset_industry_map.dta", umatch(factset_industry_code)
drop if _merge==2
drop _merge

* 6. First Entry & Classification by Groups: 
egen entry_date=rowmin(bc_date cibm_date qfii_date rqfii_date)
assert !missing(entry_date)
rename entry_date _entry_date
*gen entry_date = mdy(month(dofm(_entry_date)),1,year(dofm(_entry_date)))
gen entry_date = _entry_date
format entry_date %td
drop _entry_date
gen entry="qfii" if entry_date==qfii_date
replace entry="cibm" if entry_date==cibm_date
replace entry="bc" if entry_date==bc_date
replace entry="rqfii" if entry_date==rqfii_date
replace entry_date=rqfii_date if entry_date==. & rqfii_date~=.
order english entry entry_date
bysort naics_title: egen total_industry=count(naics_title)
order  englishname naics_title up_naics6_desc

* Industry group classifications:
gen indshort=""
replace indshort="investment_advice" if regexm(naics_title,"Investment Advice")==1
replace indshort="ibank" if regexm(naics_title,"Investment Banking")==1
replace indshort="govt" if regexm(naics_title,"Monetary")==1 | regexm(naics_title,"Legisl")==1 
replace indshort="portfolio_management" if regexm(naics_title,"Portfolio M")==1
replace indshort="pension" if regexm(naics_title,"Pension")==1
replace indshort="brokers" if regexm(naics_title,"Securities Brokerage")==1
replace indshort="commercial_banking" if regexm(naics_title,"Commer")==1
replace indshort="foundations" if regexm(naics_title,"Civic and Social")==1 | regexm(naics_title,"Foundation")==1
replace indshort="university" if regexm(naics_title,"Colleges")==1
replace indshort="insurance" if regexm(naics_title,"Insurance")==1 | regexm(naics_title,"Reinsu")==1
replace indshort="intl_orgs" if regexm(naics_title,"International Trade Financing")==1 & regexm(englishname,"CITIC")~=1 & regexm(englishname,"China General Nuclear Power Corporation Huasheng")~=1 & regexm(englishname,"Standard Chart")~=1  & regexm(englishname,"United Over")~=1 
replace indshort="intl_orgs" if englishname=="International Monetary Fund"
replace indshort="govt" if indshort=="" & regexm(up_naics6_desc,"Legisl")==1 & up_iso_cou~="CHN"
replace indshort="govt" if indshort=="" & regexm(englis,"Province")==1 & up_iso_cou~="CHN"
replace indshort="govt" if indshort=="" & regexm(up_naics6_desc,"Government")==1 & up_iso_cou~="CHN"
replace indshort="foundations" if indshort=="" & regexm(englis,"Foundation Trust")==1 & up_iso_cou~="CHN"
replace indshort="commercial_banking" if indshort=="" & regexm(up_naics6_desc,"Commercial Banking")==1 
replace indshort="portfolio_management" if indshort=="" & regexm(up_naics6_desc,"Portfolio M")==1 

*Broad groups
gen broad=indshort
replace broad="public" if indshort=="govt" | indshort=="intl_orgs"
replace broad="nonprofit" if indshort=="foundations" | indshort=="university"
gen model=broad
replace model="stable" if broad=="public" | broad=="nonprofit" | broad=="pension" | broad=="insurance"
replace model="bank" if broad=="ibank" | broad=="commercial_banking" | broad=="brokers"
replace model="portfolio" if broad=="investment_advice" | broad=="portfolio_management"
drop n manual
save "${gcap_data}/output/investor_entry/overseas_factset_full.dta", replace

* 7. CIBM Lists all entries prior to first disclosure.  Here, we assume they entered smoothly 
* Over the previous year, rather than as a discontinuity at first disclosure.  
* CIBM was launched Feb 2016 but the first disclosure of participants was February 2017
* We assume investors present by Feb 2017 entered smoothly between March 2016 and Feb 2017. 
* creating a temp file first:
use "${gcap_data}/output/investor_entry/overseas_factset_full.dta", clear
cap drop _merge
keep if entry=="cibm" & entry_date == td(01feb2017) & model == "stable"
local temp = _N
local x = `temp' / 12 // smoothing during the first year of CIMB direct
replace entry_date = td(01mar2016) if _n < `x' & entry_date == td(01feb2017)
replace entry_date = td(01apr2016) if _n < `x'*2 & entry_date == td(01feb2017)
replace entry_date = td(01may2016) if _n < `x'*3 & entry_date == td(01feb2017)
replace entry_date = td(01jun2016) if _n < `x'*4 & entry_date == td(01feb2017)
replace entry_date = td(01jul2016) if _n < `x'*5 & entry_date == td(01feb2017)
replace entry_date = td(01aug2016) if _n < `x'*6 & entry_date == td(01feb2017)
replace entry_date = td(01sep2016) if _n < `x'*7 & entry_date == td(01feb2017)
replace entry_date = td(01oct2016) if _n < `x'*8 & entry_date == td(01feb2017)
replace entry_date = td(01nov2016) if _n < `x'*9 & entry_date == td(01feb2017)
replace entry_date = td(01dec2016) if _n < `x'*10 & entry_date == td(01feb2017)
replace entry_date = td(01jan2017) if _n < `x'*11 & entry_date == td(01feb2017)
save "${gcap_data}/temp/temp_dates.dta", replace

* appending smooth version:
use "${gcap_data}/output/investor_entry/overseas_factset_full.dta", clear
drop if entry=="cibm" & entry_date == td(01feb2017) 
cap drop _merge
append using "${gcap_data}/temp/temp_dates.dta"
save "${gcap_data}/output/investor_entry/overseas_factset_full_smooth.dta", replace
cap rm "${gcap_data}/temp/temp_dates.dta"

cap log close

