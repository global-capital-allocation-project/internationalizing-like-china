******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/reserves_estimate.log", replace

* Install required packages
ssc install kountry

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/foreign_holdings"
cap mkdir "${gcap_data}/output/foreign_holdings/temp"

******************************************************************************************************
* PREPARING DATASETS USED IN THE ANALYSIS
******************************************************************************************************

* Hand Collected Data For Reserves obtained from Official Documents
import excel using "${gcap_data}/input/reserves/manual_reserves.xlsx", clear cellrange(A1:K90) firstrow
keep country year usdvaluemill
rename country iso_country
rename usdvaluemill rmb_manual
replace rmb_manual = rmb_manual / 1e3
drop if missing(rmb_manual)
save "${gcap_data}/output/foreign_holdings/temp/reserves_official_documents.dta", replace

* Obtaining Chinese Reserves from IFS:
use "${gcap_data}/input/reserves/IFS_full.dta", clear
keep if attribute == "Value"
drop attribute
keep if indicator_code == "RAXGFX_USD"
qui destring y*, force replace
qui reshape long y, i(country_name	country_code indicator_name	indicator_code) j(value)
drop indicator_code indicator_name
kountry country_code, from(imfn) to(iso3c)
drop if missing(_ISO3C_)
drop country_name country_code
keep value y _ISO3C_
rename value year 
rename y value
keep if year > 2010 & year < 2023
rename value ifs_fx_reserves
rename _ISO3C_ iso_country
order iso_country year ifs_fx_reserves
sort iso year
replace ifs = ifs/1e6
keep if iso_country == "CHN"
rename ifs_fx_reserves chn_reserves
gen quarter = yq(year, 4)
format %tq quarter
drop year
save "${gcap_data}/output/foreign_holdings/temp/ifs_chinese_reserves.dta", replace

******************************************************************************************************
* Obtaining Aggregate Reserves Estimate: COFER + Survey
******************************************************************************************************

* Processing COFER data: Create Series of total reserve holdings
use "${gcap_data}/input/reserves/cofer.dta", clear
qui mmerge quarter using "${gcap_data}/input/miscellaneous/survey_rmb_total.dta", unmatched(m)
replace rmb=survey if _merge==3
drop _merge
qui mmerge quarter using "${gcap_data}/output/foreign_holdings/temp/ifs_chinese_reserves.dta"
gen q = quarter(dofq(quarter))
keep if q == 4
drop q

* China became a COFER reporter from 2015 to 2018. Chinese reserves enter COFER data gradually between 2015 and 2018.
* Assumption that share included increases by 25% per year.
drop if quarter < tq(2013q4)
gen total_allocated_ex_chn = total_allocated
replace total_allocated_ex_chn = total_allocated - 0.25*chn_reserves if quarter == tq(2015q4)
replace total_allocated_ex_chn = total_allocated - 0.5*chn_reserves if quarter == tq(2016q4)
replace total_allocated_ex_chn = total_allocated - 0.75*chn_reserves if quarter == tq(2017q4)
replace total_allocated_ex_chn = total_allocated - chn_reserves if quarter >= tq(2018q4)

gen unallocated_ex_chn = unallocated
replace unallocated_ex_chn = unallocated - chn_reserves if quarter<tq(2015q4)
replace unallocated_ex_chn = unallocated - 0.75*chn_reserves if quarter == tq(2015q4)
replace unallocated_ex_chn = unallocated - 0.5*chn_reserves if quarter == tq(2016q4)
replace unallocated_ex_chn = unallocated - 0.25*chn_reserves if quarter == tq(2017q4)

gen share_cny_ex_chn = rmb / total_allocated_ex_chn
replace share_cny = rmb / total_allocated if missing(share_cny)
ipolate share_cny_ex_chn quarter, gen (i_share_cny_ex_chn)

drop rmb
replace share_cny_ex_chn = i_share_cny_ex_chn if missing(share_cny_ex_chn)
drop i_*
gen rmb_allocated=share_cny_ex_chn*total_allocated_ex_chn
gen rmb_unallocated=share_cny_ex_chn*unallocated_ex_chn
keep quarter rmb_allocated rmb_unallocated

* convert to billions
replace rmb_allocated = rmb_allocated/1000
replace rmb_unallocated = rmb_unallocated/1000
rename rmb_allocated cofer_cny_allocated
rename rmb_unallocated cofer_cny_unallocated
gen year = year(dofq(quarter))
keep cofer* year
order year, first
gen reserves_total=cofer_cny_unallocated+cofer_cny_allocated
* saving total reserves breakdown by cofer allocated and unallocated
save "${gcap_data}/output/foreign_holdings/reserves_estimate.dta", replace

******************************************************************************************************
* Obtaining Reserves Estimate By Country: Combining SDDS and Official Documents
******************************************************************************************************

cap restore
import delim using "${gcap_data}/input/reserves/SDDS_full_update.csv", clear encoding(UTF-8)
unab vars: v*

local i = 1995
local count = 1

foreach v of varlist `vars' {
    rename `v' y`i'
    local i=`i'+`count'
    local `count'=`count'+1
}

cap drop v2023
keep if attribute=="Value"
drop attribute
qui reshape long y, i(countryname	countrycode	indicatorname	indicatorcode	sectorname	sectorcode) j(year)

rename y value
drop if year==2023
drop if missing(value)

replace value=value/1e9
drop if countrycode == 924 //China

* keeping variables of interest:
keep if sectorcode =="MCG"
drop sectorcode sectorname
replace countrycode = 163 if countrycode == 168 // put the european central bank in the euro-zone
drop if countrycode == 487 // West Bank and Gaza
collapse (sum) value, by(indicatorcode year countrycode indicatorname)
keep if  inlist(indicatorcode,"RAMCRIC_CNY_USD")
replace indicatorcode =subinstr(indicatorcode,"RAMCRIC_","",.)
replace indicatorcode =subinstr(indicatorcode,"_USD","",.)
drop indicatorname
reshape wide value, i(countrycode year) j(indicatorcode, string)
renpfix value
kountry countrycode,  from(imfn) to(iso3c)
nmissing(_ISO3C_)
gen iso_country = _ISO3C_
drop _ISO3C_
order iso year countrycode
drop countrycode
* merging reserves obtained from manual searches
qui mmerge iso_country year using "${gcap_data}/output/foreign_holdings/temp/reserves_official_documents.dta"
gen manual = 0
replace manual = 1 if rmb_manual != .
keep iso_country year CNY rmb_manual manual
keep if year > 2012 & year < 2023
rename CNY rmb_sdds

* Merge in list of countries that report to COFER
mmerge iso_country using "${gcap_data}/input/reserves/cofer_reporters_list.dta", umatch(iso3) unmatched(m) ukeep(cofer_reporter)
replace cofer_reporter = 0 if missing(cofer_reporter)
drop _merge
duplicates drop
gsort iso year
gen sdds_reporter = 0
replace sdds_reporter = 1 if !missing(rmb_sdds)
replace cofer_reporter =1 if sdds_reporter == 1
gen rmb_identifiable = rmb_sdds
replace rmb_identifiable = rmb_manual if missing(rmb_identifiable)
bys year cofer_reporter: egen total_cofer_observable = total(rmb_identifiable) if cofer_reporter == 1
sort iso_country year
mmerge year using "${gcap_data}/output/foreign_holdings/reserves_estimate.dta"
keep if year > 2013 & year < 2023
drop _merge
gsort iso_ year
rename iso_country iso_country_code
gen reporter = sdds_reporter + manual
gen source = "SDDS" if sdds_reporter == 1
replace source = "Official Documents" if manual == 1
keep if reporter == 1
keep iso year rmb_identifiable source
save "${gcap_data}/output/foreign_holdings/temp/reserves_reporters.dta", replace // manual or sdds

* obtaining reserves identifiable by country
collapse (sum) rmb_identifiable, by(year)
rename rmb_identifiable cofer_observable
mmerge year using "${gcap_data}/output/foreign_holdings/reserves_estimate.dta"
drop if year < 2014
drop _merge
gen other_reserves = reserves_total - cofer_observable
drop reserves_total cofer_observable cofer_cny_allocated cofer_cny_unallocated
reshape long other_, i(year) j(iso_country_code, string)
rename other_ estimate_reserves
replace iso_country_code = "Other Reserves"
append using "${gcap_data}/output/foreign_holdings/temp/reserves_reporters.dta"
replace estimate_reserves = rmb_identifiable if !missing(rmb_identifiable)
drop rmb_identifiable 
replace estimate_reserves = estimate_reserves*1000
replace estimate_reserves = round(estimate_reserves)
replace source = "Author's Calculations based on COFER" if regexm(iso_country_code,"Other")
rename source source_reserves
* saving reserves estimate by country
save "${gcap_data}/output/foreign_holdings/reserves_estimate_by_country.dta", replace

cap log close