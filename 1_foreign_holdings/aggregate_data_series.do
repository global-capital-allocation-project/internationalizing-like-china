******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do "${gcap_data}/rmb_replication/Project_globals.do"

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/aggregate_data_series.log", replace

* Install required packages
ssc install kountry
ssc install sxpose

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/foreign_holdings"
cap mkdir "${gcap_data}/output/foreign_holdings/temp"
cap mkdir "${gcap_data}/output/foreign_holdings/temp/shch"


******************************************************************************************************
* CLEANING AND COMBINING FILES FOR Shanghai Clearing House (SHCH)
******************************************************************************************************

cap restore
clear

local files : dir "${gcap_data}/input/shch/shch_en/" files "en_js.xlsx"
local files = ustrregexra(`"`files'"', "en_js.xlsx", "")

    foreach file of local files {
        display "`file'"
        qui import excel "${gcap_data}/input/shch/shch_en/`file'", clear sheet("Table 5") allstring
        local i = 1
        qui ds
        foreach var of varlist `r(varlist)' {
            qui replace `var' = lower(`var')
            qui rename `var' var`i'
            local i = `i' + 1
        }
        * keeping foreign institutions
        qui keep if ustrregexm(var1, "overseas\s+institution") | ustrregexm(var1, "foreign\s+institution")
        * summing all entries
        qui ds
        local allvars "`r(varlist)'"
        qui des
        local lastvar: word `r(k)' of `allvars'
        qui destring var2-`lastvar', replace
        qui egen shch_total_rmb = rowtotal(var2-`lastvar')
        */list
        keep var1 shch_total_rmb
        gen period = regexr("`file'", "_en_js.xlsx", "")
        gen date_m = ym(real(substr(period,1,4)),real(substr(period,5,2)))
        drop period
        format %tm date_m
        local file = ustrregexra("`file'", "_en_js.xlsx", "")
        qui save "${gcap_data}/output/foreign_holdings/temp/shch/tmp_`file'.dta", replace
    }

    local allfiles : dir "${gcap_data}/output/foreign_holdings/temp/shch" files "*.dta"
    display `allfiles'

    clear
    foreach file of local allfiles {
        append using "${gcap_data}/output/foreign_holdings/temp/shch/`file'"
    }
    sort date_m
    replace shch_total_rmb = shch_total_rmb / 10 //series in billions (originally in hundreds of billions of RMB)
    keep date_m shch_total_rmb
    order date_m, first
    save "${gcap_data}/output/foreign_holdings/temp/shch_total_rmb.dta", replace
    cap rmdir "${gcap_data}/output/foreign_holdings/temp/shch"

******************************************************************************************************
* CLEANING AND COMBINING FILES FOR China Central Depository & Clearing (CCDC)
******************************************************************************************************

cap restore
clear 
local allfiles : dir "${gcap_data}/input/ccdc" files "*.xlsx"
display `allfiles'

cap rm "${gcap_data}/output/foreign_holdings/temp/temp_ccdc.dta"

clear

foreach file in `allfiles' {
    preserve
    qui import excel using "${gcap_data}/input/ccdc/`file'", clear firstrow allstring
    local noextension=subinstr("`file'",".xlsx","",.)
    rename B ccdc_total_rmb
    replace A = lower(A)
    qui keep if regexm(A, "foreign investor") | regexm(A, "external institution")
    keep A ccdc_total_rmb
    qui gen period = "`noextension'"
    qui save "${gcap_data}/output/foreign_holdings/temp/temp_ccdc.dta", replace
    restore
    append using "${gcap_data}/output/foreign_holdings/temp/temp_ccdc.dta"
}

rm "${gcap_data}/output/foreign_holdings/temp/temp_ccdc.dta"
sort period
tostring period, replace
destring ccdc_total_rmb, replace
replace ccdc_total_rmb = ccdc_total_rmb / 10 //series in billions (originally in hundreds of millions)
keep period ccdc_total_rmb
gen date_m = ym(real(substr(period,1,4)),real(substr(period,5,2)))
format %tm date_m
drop period
save "${gcap_data}/output/foreign_holdings/temp/ccdc_total_rmb.dta", replace

******************************************************************************************************
* HOLDING COMPOSITION DATA: China Central Depository & Clearing (CCDC) 
******************************************************************************************************

import excel "${gcap_data}/input/ccdc/composition/202112_T06.xlsx", sheet("sheet1") clear
drop if _n<=3
forvalues i=1/7 {
	replace A=subinstr(A,"`i'.","",.)
}
replace A=subinstr(A,"I.","",.)
replace A="Interbank Market" if regexm(A,"Inter")==1
replace A="OTC Market" if regexm(A,"OTC Mar")==1
replace A="Other Markets" if regexm(A,"Other Market")==1
replace A=trim(A)
replace A=subinstr(A," ","_",.)
drop if _n>=13
sxpose, clear
replace _var1="bondtype" if _n==1
foreach x of varlist _all {
	local temp=subinstr(`x'[1]," ","_",.)
	local temp2=lower("`temp'")
	rename `x' `temp2'
}
drop if _n==1

foreach x in total interbank_market commercial_banks credit_cooperatives insurance_institutions securities_companies unincorporated_products foreign_investors others otc_market other_markets {
	destring `x', replace
}

set obs 7
replace bondtype = "Aggregate" in 7
foreach x in total interbank_market commercial_banks credit_cooperatives insurance_institutions securities_companies unincorporated_products foreign_investors others otc_market other_markets {
	summ `x'
	replace `x'=r(sum) if bondtype=="Aggregate"
}	

foreach x in interbank_market commercial_banks credit_cooperatives insurance_institutions securities_companies unincorporated_products foreign_investors others otc_market other_markets {
	gen share_`x'=`x'/total
}
save "${gcap_data}/output/foreign_holdings/holdings_decomposition.dta", replace

******************************************************************************************************
* CLEANING BOND CONNECT AGGREGATE HOLDINGS
******************************************************************************************************

import excel "${gcap_data}/input/bond_connect/bc_holdings.xlsx", firstrow clear
destring shch ccdc total, replace ignore(",")

* Convert the integer date variable to string format
gen str month_str = string(month, "%tdMon-YY")

* Adjust the string to have a proper format: "01May2022" for instance
gen proper_date_str = "01" + substr(month_str, 1, 3) + "20" + substr(month_str, 5, 2)

* Extract month and year from proper_date_str
gen int month_num = month(date(proper_date_str, "DMY"))
gen int year_num = year(date(proper_date_str, "DMY"))

* Convert month and year into a Stata monthly date
gen new_date = mofd(mdy(month_num, 1, year_num))

* Format the new date
format new_date %tm
drop month* proper_date_str year_num 
rename new_date date_m
order date_m, first

* Save
sort date_m
save "${gcap_data}/output/foreign_holdings/temp/bc_temp_agg.dta", replace

******************************************************************************************************
* MERGING SHCH AND CCDS FOR LONGER BOND CONNECT SERIES
******************************************************************************************************

use "${gcap_data}/output/foreign_holdings/temp/bc_temp_agg.dta", clear
* Merge data from SHCH and CCDC for years before 2017
qui mmerge date_m using "${gcap_data}/output/foreign_holdings/temp/shch_total_rmb.dta"
qui mmerge date_m using "${gcap_data}/output/foreign_holdings/temp/ccdc_total_rmb.dta"
* Merging exchange rate for USD amount
qui mmerge date_m using "${gcap_data}/input/miscellaneous/fx_rate_eom.dta", unmatched(m)
drop _merge
sort date_m
replace shch_total_rmb = shch if missing(shch_total_rmb) & !missing(shch)
replace ccdc_total_rmb = ccdc if missing(ccdc_total_rmb) & !missing(ccdc)
gen bc_total_rmb = shch_total_rm + ccdc_total_rmb
drop shch	ccdc	total	
gen bc_total_usd = bc_total_rmb / dexchus
keep if date_m >= tm(2014m12)
rename dexchus fx_rate

* Saving monthly aggregate
save "${gcap_data}/output/foreign_holdings/aggregate_holdings_m.dta", replace

* Saving quarterly aggregate
keep if inlist(month(dofm(date_m)),3,6,9,12)
save "${gcap_data}/output/foreign_holdings/aggregate_holdings_q.dta", replace

* Saving annual aggregate
gen year=year(dofm(date_m))
keep if month(dofm(date_m))==12
drop date_m
order year, first
save "${gcap_data}/output/foreign_holdings/aggregate_holdings_y.dta", replace

cap log close
