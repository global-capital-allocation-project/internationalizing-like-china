******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
qui do Project_globals.do

* install required package
ssc install listtex

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/index_inclusion.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/index_inclusion"
cap mkdir "${gcap_data}/output/index_inclusion/temp"


global flows_direct "${gcap_data}/input/morningstar/morningstar_direct_supplementary"
global dptmp "${gcap_data}/output/index_inclusion/temp"

******************************************************************************************************
* IMPORT SUPPLEMENTARY DATA FROM MORNINGSTAR DIRECT
******************************************************************************************************

//- import OpenFunds (FO)

local files : dir "${flows_direct}/OpenFunds" files "*.csv"
local fo_append

foreach file in `files' {
    
    di "Importing `file'"
	qui filefilter "${flows_direct}/OpenFunds/`file'" "${dptmp}/`file'", from(\n) to("") replace
    qui import delimited "${dptmp}/`file'", clear delimiters(",") bindquote(strict) varnames(nonames)

    foreach var of varlist * {
        local try = `var'[1]
        if "`var'" == "v1" {
            local try = "Name" 
        }
        local try = subinstr("`try'", char(10), "", .)
        local try = subinstr("`try'", "Estimated Fund-Level Net Flow - comprehensive (Monthly)", "NetFlow", .)
        local try = subinstr("`try'", "Fund Size - comprehensive (Monthly)", "FundSize", .)
        local try = subinstr("`try'", " USD", "", .)
        local try = subinstr("`try'", " ", "_", .)
        local try = subinstr("`try'", "Morningstar_Category_2021-08", "Morningstar_Category", .)
        local try = subinstr("`try'", "-", "_", .)
        local try = subinstr("`try'", "Primary_Prospectus_Benchmark_Inception_Date", "Primary_Benchmark_Inception", .)    
        cap rename `var' `try'
    }

    qui drop if _n == 1
    cap drop v109
    qui drop if Name == "Summary Statistics" & missing(SecId)
    qui drop if Name == "Sum" & missing(SecId)
    qui drop if Name == "Maximum" & missing(SecId)
    qui drop if Name == "Minimum" & missing(SecId)
    qui drop if Name == "Median" & missing(SecId)
    qui drop if Name == "Standard Deviation" & missing(SecId)
    qui drop if Name == "Count"

	local file = regexr(`"`file'"', ".csv", "")
    qui save "${dptmp}/toApp_`file'.dta", replace
	local fo_append `fo_append' toApp_`file'.dta

}

//- import ETF Funds (FE)

local files : dir "${flows_direct}/ETF" files "*.csv"
local fe_append

foreach file in `files' {
	
    di "Importing `file'"
	
	qui filefilter "${flows_direct}/ETF/`file'" "${dptmp}/`file'", from(\n) to("") replace
    qui import delimited "${dptmp}/`file'", clear delimiters(",") bindquote(strict) varnames(nonames)

    foreach var of varlist * {
        local try = `var'[1]
        if "`var'" == "v1" {
            local try = "Name" 
        }
        local try = subinstr("`try'", char(10), "", .)
        local try = subinstr("`try'", "Estimated Fund-Level Net Flow - comprehensive (Monthly)", "NetFlow", .)
        local try = subinstr("`try'", "Fund Size - comprehensive (Monthly)", "FundSize", .)
        local try = subinstr("`try'", " USD", "", .)
        local try = subinstr("`try'", " ", "_", .)
        local try = subinstr("`try'", "Morningstar_Category_2021-08", "Morningstar_Category", .)
        local try = subinstr("`try'", "-", "_", .)
        local try = subinstr("`try'", "Primary_Prospectus_Benchmark_Inception_Date", "Primary_Benchmark_Inception", .)    
        cap rename `var' `try'
    }

    qui drop if _n == 1
    cap drop v109
    qui drop if Name == "Summary Statistics" & missing(SecId)
    qui drop if Name == "Sum" & missing(SecId)
    qui drop if Name == "Maximum" & missing(SecId)
    qui drop if Name == "Minimum" & missing(SecId)
    qui drop if Name == "Median" & missing(SecId)
    qui drop if Name == "Standard Deviation" & missing(SecId)
    qui drop if Name == "Count"

	local file = regexr(`"`file'"', ".csv", "")
    qui save "${dptmp}/toApp_`file'.dta", replace
	local fe_append `fe_append' toApp_`file'.dta

}

//- consolidating all funds

* consolidate open funds

clear
qui gen FundType = ""

foreach file in `fo_append' {
    qui append using "${dptmp}/`file'"
}
qui replace FundType = "FO" if missing(FundType)


* consolidate etf funds

foreach file in `fe_append' {
    qui append using "${dptmp}/`file'"
}
qui replace FundType = "FE" if missing(FundType)

* save
save "${dptmp}/morningstar_dm9_flows_consolidated", replace

******************************************************************************************************
* MORNINGSTAR DIRECT SUPPLEMENT CLEANING AND COLLAPSING
******************************************************************************************************

use "${dptmp}/morningstar_dm9_flows_consolidated", clear
duplicates report SecId
duplicates drop SecId, force

* saving raw data at the SecId level to work with in case of need.
save "${dptmp}/morningstar_dm9_raw_SecId_level", replace

gsort -FundId -SecId

* number of distinct benchmarks per fund
by FundId Primary_Prospectus_Benchmark, sort: gen nvals_bm = _n == 1 
by FundId : replace nvals = sum(nvals)
by FundId : replace nvals = nvals[_N] 
tab nvals 
drop nvals

* number of 'IndexFund' securities per fund.
bys FundId Index_Fund : gen nif = _n == 1
bys FundId : replace nif = sum(nif)
bys FundId : replace nif = nif[_N]
tab nif
drop nif


* destring numeric vars
foreach var of varlist NetFlow_2017_12-FundSize_2021_08 {
    
    display ". `var'"
    qui replace `var' = subinstr(`var', ",", "", .)
    qui destring `var', replace
    
}

* mean of FundSize accross securities in a Fund
preserve
collapse (mean) FundSize_* , by(FundId)
rename FundSize_* mean_size_*
save "${dptmp}/tmp_mean.dta", replace
restore

* first entry of FundSize in a Fund
gsort FundId -ISIN
collapse (first) FundType Name ISIN Primary_Prospectus_Benchmark Primary_Prospectus_Benchmark_Id Index_Fund ///
                 FundSize_*, by(FundId)
rename FundSize_* first_size_*
merge 1:1 FundId using "${dptmp}/tmp_mean.dta"
drop _merge

* max % difference between 'mean' and 'first'

local order

foreach var of varlist first_size_2017_12-first_size_2021_08 {
    local m = ustrregexra("`var'", "first_size_", "")
    local order `order' *`m'
    qui gen p_`m' = 100 * (`var' - mean_size_`m') / mean_size_`m'
}

order `order', after(Index_Fund)
qui egen max = rowmax(p_*)
qui gen prob = cond(max > 1, 1, 0) if ! missing(max) // max == . are those funds for which we don't have fund size data (non-relevant).

* checking benchmarked value for problematic funds (prob == 1)
preserve
collapse (sum) mean_size*, by(prob)
drop if missing(prob)

foreach var of varlist mean_size* {
    local m = ustrregexra("`var'", "mean_size_", "")
    qui egen tot_`m' = total(`var')
    qui gen p_`m' = `var' / tot_`m'
    
}

qui keep if prob == 1
qui egen max = rowmax(p_*)
display "... proportion of value in proplematic funds (max)"
tab max, m
restore

* keeping only ID vars + one set of variables that identify fund size

drop first_size* p_* max prob
rename mean_size* FundSize*
gsort -FundId -Name

* saving FundSize data (already at the fund, not at the security level)
duplicates report FundId
save "${dptmp}/tmp_fundSize_fundLevel.dta", replace


******************************************************************************************************
* BENCHMARKS
******************************************************************************************************

use "${dptmp}/tmp_fundSize_fundLevel.dta", clear

keep FundId-Index_Fund FundSize_2021_06
replace Primary_Prospectus_Benchmark = strlower(Primary_Prospectus_Benchmark)


* getting full list of benchmark names with benchmark ids (to have them as handy helpers)
preserve
qui gen missprosp = missing(Primary_Prospectus_Benchmark_Id)
qui keep if missprosp == 1
duplicates drop Primary_Prospectus_Benchmark, force
sort Primary_Prospectus_Benchmark

*save tmp to check
export excel using "${dptmp}/tmp_tocheck_missIDs", replace firstrow(variables)
restore

* saving handy temp data to work with in subsequent steps
gen miss_bm_id = missing(Primary_Prospectus_Benchmark_Id)
tab miss_bm_id, m
gen rmb_index = ""
save "${dptmp}/tmp_handydata_to_flag_pre.dta", replace

* importing manually checked index and saving in stata file

import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/bloomberg_main_0_master.xls", clear firstrow allstring
import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/bloomberg_main_0_master.xlsx", clear firstrow allstring
drop if missing(tmp_handler)
keep tmp_handler FundId flag_decision flag_index_isin documents_folder flag_description flag_url
rename tmp_handler old_rmb_index
tab flag_decision, m

* save
save "${dptmp}/tmp_1.dta", replace

**selection 2: bloomberg others**

cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/bloomberg_cand_0_master.xls", clear firstrow allstring
cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/bloomberg_cand_0_master.xlsx", clear firstrow allstring
drop if missing(tmp_handler)
keep tmp_handler FundId flag_decision flag_index_isin documents_folder flag_description flag_url
rename tmp_handler old_rmb_index
tab flag_decision, m

* save
save "${dptmp}/tmp_2.dta", replace

**selection 3: jpm**

cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/jpm_main_0_master.xls", clear firstrow allstring
cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/jpm_main_0_master.xlsx", clear firstrow allstring
drop if missing(tmp_handler)
keep tmp_handler FundId flag_decision flag_index_isin documents_folder flag_description flag_url
rename tmp_handler old_rmb_index
tab flag_decision, m

* save
save "${dptmp}/tmp_3.dta", replace

**selection 4: jpm others**

cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/jpm_cand_0_master.xls", clear firstrow allstring
cap import excel using "${gcap_data}/input/morningstar/morningstar_direct_supplementary/jpm_cand_0_master.xlsx", clear firstrow allstring
drop if missing(tmp_handler)
keep tmp_handler FundId flag_decision flag_index_isin documents_folder flag_description flag_url
rename tmp_handler old_rmb_index
tab flag_decision, m

* save
save "${dptmp}/tmp_4.dta", replace

**Merging with main data of indexes**

use "${dptmp}/tmp_1.dta", clear
append using "${dptmp}/tmp_2.dta"
append using "${dptmp}/tmp_3.dta"
append using "${dptmp}/tmp_4.dta"
keep old_rmb_index FundId flag_decision

drop if old_rmb_index == "no candidates"

* merging with main fund level data
merge 1:1 FundId using "${dptmp}/tmp_handydata_to_flag_pre.dta"
drop _merge

* for each Primary_Prospectus_Benchmark, leave the filled flag_decision in first entry
gsort Primary_Prospectus_Benchmark -flag_decision

* filling flag_decision var
bys Primary_Prospectus_Benchmark : replace flag_decision = flag_decision[1]
bys Primary_Prospectus_Benchmark : replace old_rmb_index = old_rmb_index[1]

* save tmp
save "${dptmp}/tmp_data.dta", replace

**flagging indexes according to flag_decision**

use "${dptmp}/tmp_data.dta", clear
replace old_rmb_index = "bloomberg" if regexm(old_rmb_index, "bloomberg")
replace old_rmb_index = "jpm" if regexm(old_rmb_index, "jpm")
tab flag_decision old_rmb_index if old_rmb_index == "bloomberg"
tab flag_decision old_rmb_index if old_rmb_index == "jpm"

* indicator of family_of_indexes
gen rmb_index_fam = ""

* bloomberg flags
qui replace rmb_index = "bloomberg (other)" if regexm(flag_decision, "drop") & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate credit usd denom"  if flag_decision == "drop (USD index)" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg ausbond"  if flag_decision == "drop (ausbond)" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate corp usd denom" if flag_decision == "drop (corp USD denom)" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global high yield" if flag_decision == "drop (global HY)" & old_rmb_index == "bloomberg"
qui replace rmb_index = "bloomberg" if regexm(flag_decision, "include") & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate" if flag_decision == "include" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate corporate" if flag_decision == "include (flag corporate)" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate credit" if flag_decision == "include (flag credit)" & old_rmb_index == "bloomberg"
qui replace rmb_index_fam = "bloomberg global aggregate treasuries" if flag_decision == "include (treasuries)" & old_rmb_index == "bloomberg"
qui replace rmb_index = "bloomberg (dismissed by size)" if flag_decision == "dismissed candidate" & old_rmb_index == "bloomberg" // rmb_index = "bloomberg (dismissed by size)"
qui replace rmb_index = "bloomberg (dismissed by size)" if flag_decision == "irrelevant size" & old_rmb_index == "bloomberg" // rmb_index = "bloomberg (dismissed by size)"

* jpm flags
qui replace rmb_index = "jpm (other)" if regexm(flag_decision, "drop") & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi broad" if flag_decision == "drop (broad)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi emu" if flag_decision == "drop (emu)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi global" if flag_decision == "drop (global, not EM)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "non relevant" if flag_decision == "drop (other)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi us" if flag_decision == "drop (us)" & old_rmb_index == "jpm"
qui replace rmb_index = "jpm" if regexm(flag_decision, "include") & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi em global composite"  if flag_decision == "include (global composite)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi em global core"  if flag_decision == "include (global core)" & old_rmb_index == "jpm"
qui replace rmb_index_fam = "jpm gbi em global diversified"  if flag_decision == "include (global diversified)" & old_rmb_index == "jpm"
qui replace rmb_index = "jpm (dismissed by size)" if flag_decision == "dismissed candidate" & old_rmb_index == "jpm" // rmb_index = "jpm (dismissed by size)"
qui replace rmb_index = "jpm (dismissed by size)" if flag_decision == "irrelevant size" & old_rmb_index == "jpm" // rmb_index = "jpm (dismissed by size)"


bys old_rmb_index: tab rmb_index, m
drop old_rmb_index flag_decision

* save
save "${dptmp}/tmp_handydata_to_flag.dta", replace


use "${dptmp}/tmp_handydata_to_flag.dta", clear 

//- all composite indexes
preserve
keep if ustrregexm(Primary_Prospectus_Benchmark, "^\(.*?\)") & miss_bm_id == 1
duplicates drop Primary_Prospectus_Benchmark, force
display _N
list Primary_Prospectus_Benchmark in 1/15
keep Primary_Prospectus_Benchmark
sort Primary_Prospectus_Benchmark
export excel using "${dptmp}/tmp_check", replace firstrow(variables)
restore

* all these are composite indexes (checked them manually in file tmp_check.xlsx) which, after discussion with Jesse, we decided to dismiss.
replace rmb_index = "non-relevant (composite)" if ustrregexm(Primary_Prospectus_Benchmark, "^\(.*?\)") & miss_bm_id == 1

* checking all have identifiers (FundId should be the identifier of the fund)
duplicates report FundId

* save matched observations WITH index ID
keep if rmb_index == "non-relevant (composite)"
tab rmb_index, m
save "${dptmp}/toappend_composite_indexes.dta", replace


******************************************************************************************************
* BLOOMBERG GLOBAL AGGREGATE
******************************************************************************************************

use "${dptmp}/tmp_handydata_to_flag.dta", clear 
//- filling values of observations that were already filled in previous steps
merge 1:1 FundId using "${dptmp}/toappend_composite_indexes.dta", keepusing(rmb_index) update

display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* checking data is at the fund level
duplicates report FundId

* keeping only: 1) observations that have missing rmb_index or 2) observations that already have bloomberg flag (to use as comparison for possible candidate indexes)
keep if missing(rmb_index) | ustrregexm(rmb_index, "bloomberg")
tab rmb_index, m

* handler variables for index names
gen cand_included = ""
gen cand_benchmark = ""

**"bloomberg global aggregate" matches, without term "barclays"**

//- "bloomberg\s*global\s*aggregate" 

* checking those that will be considered in index
preserve
keep if ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*global\s*aggregate")
duplicates drop Primary_Prospectus_Benchmark, force
display ". prospectus and main classification"
replace rmb_index = "filled" if ! missing(rmb_index)
list Primary_Prospectus_Benchmark rmb_index
display ". families of indexes"
tab rmb_index_fam
restore

* all already filled

//- comparing value of funds that index possible "bloomberg" candidates with those that are already flagged. 
* creating flag for already filled indexes and possible candidates
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ( ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*gbl\s*agg") | ///
                        ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*global\s*agg") ) & ///
                      missing(rmb_index)

* finding value of funds per candidate index
cap drop val
sort filled Primary_Prospectus_Benchmark
bys filled Primary_Prospectus_Benchmark: egen val = total(FundSize_2021_06)

* list of candidates possible benchmarks and value with respect to total already benchmarked
preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) val ISIN, by(filled Primary_Prospectus_Benchmark)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val

gen prop = val / tot

gen o = _n
list Primary_Prospectus_Benchmark ISIN prop o in 1/10

* selecting flagged indexes.
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 1, 4, 10), local(include)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 2, 9), local(include_corp)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 5), local(include_credit)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 6), local(include_treas)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 3, 7, 8), local(dismissed)
restore

* included
foreach bm of local include {
    qui replace rmb_index = "bloomberg" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index_fam = "bloomberg global aggregate" if Primary_Prospectus_Benchmark == "`bm'"
}

foreach bm of local include_corp {
    qui replace rmb_index = "bloomberg" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index_fam = "bloomberg global aggregate corporate" if Primary_Prospectus_Benchmark == "`bm'"
}

foreach bm of local include_credit {
    qui replace rmb_index = "bloomberg" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index_fam = "bloomberg global aggregate credit" if Primary_Prospectus_Benchmark == "`bm'"
}

foreach bm of local include_treas {
    qui replace rmb_index = "bloomberg" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index_fam = "bloomberg global aggregate treasuries" if Primary_Prospectus_Benchmark == "`bm'"
}

* dismissed
foreach bm of local dismissed {
    qui replace rmb_index = "bloomberg (dismissed, not checked)" if Primary_Prospectus_Benchmark == "`bm'"
}

//- checking value of funds in benchmark

preserve
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ( ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*gbl\s*agg") | ///
                        ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*global\s*agg") ) & ///
                      missing(rmb_index)

collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot
restore

* we are dismissing less than 1% of total funds if we leave the rest as non-matches. I decide to dismiss the rest.

replace rmb_index = "bloomberg (dismissed, not checked)" if ( ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*gbl\s*agg") | ///
                                                              ustrregexm(Primary_Prospectus_Benchmark, "bloomberg\s*global\s*agg") ) & ///
                                                              missing(rmb_index)

**"bloomberg" matches**

* identified bloomberg vs unidentified to check

cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bloomberg") & missing(rmb_index)

* general family index

cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(bloomberg\s*[a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if ! inlist(o, 8, 33, 53, 54, 55, 62, 63, 64) & o <= 70, local(others)
restore

* flag others

foreach bm of local others {
     qui replace rmb_index = "bloomberg (other)" if index_fam == "`bm'"
}


* identified bloomberg vs unidentified to check

cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bloomberg") & missing(rmb_index)

* general family index

cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(bloomberg\s*[a-z0-9]+\s*[a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n

* flagging indexes to check and indexes to dismiss

qui levelsof index_fam if o <= 20, local(others)
restore

* flag others

foreach bm of local others {
     qui replace rmb_index = "bloomberg (other)" if index_fam == "`bm'"
}

//- checking value of funds that are 'bloomberg' and haven't been flagged

preserve
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bloomberg") & missing(rmb_index)
collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot
restore

* less than 1% of the funds dismissed. Flagged as dismissed.

replace rmb_index = "bloomberg (dismissed, not checked)" if ustrregexm(Primary_Prospectus_Benchmark, "bloomberg") & missing(rmb_index)

**Bloomberg abbreviation "bbg"**

* identified bloomberg vs unidentified to check

cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bbg") & missing(rmb_index)
* general family index
cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(bbg\s*[a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if inlist(o, 2, 3, 4, 6, 8, 9, 10, 11), local(others)
restore

* flag others

foreach bm of local others {
     qui replace rmb_index = "bloomberg (other)" if index_fam == "`bm'"
}



* identified bloomberg vs unidentified to check
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bbgbarc\s[^g]") & missing(rmb_index)
* general family index
cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(bbgbarc\s[^g][a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve

gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if ! inlist(o, 33) & o <= 50, local(others)
restore

* flag others

foreach bm of local others {
     qui replace rmb_index = "bloomberg (other)" if index_fam == "`bm'"
}

//- checking value of funds that may include other important terms in benchmark

preserve
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "bbg") & missing(rmb_index)
collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot

restore

replace rmb_index = "bloomberg (dismissed, not checked, prob1)" if ustrregexm(Primary_Prospectus_Benchmark, "bbg") & missing(rmb_index)

**Other possible bloomberg/barclays indexes**

//- checking value of funds that may include other important terms in benchmark

preserve
cap drop filled
gen filled = 1 if rmb_index == "bloomberg"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "barc") & missing(rmb_index)
collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot
restore

* can be dismissed (less than 4% of the funds value). flag as 'prob1' as 7% may become a big part when accumulating dismissed.
replace rmb_index = "bloomberg (dismissed, not checked)" if ustrregexm(Primary_Prospectus_Benchmark, "barc") & missing(rmb_index)

* none other possible regex. Sample is complete.

* checking all have identifiers (FundId should be the identifier of the fund)
duplicates report FundId

* save matched observations WITH index ID
cap drop filled val
keep if ! missing(rmb_index)
tab rmb_index, m
save "${dptmp}/toappend_flagged_bloomberg.dta", replace

******************************************************************************************************
* JPM GBI-EM Index
******************************************************************************************************

use "${dptmp}/tmp_handydata_to_flag.dta", clear

//- filling values of observations that were already filled in previous steps
merge 1:1 FundId using "${dptmp}/toappend_composite_indexes.dta", keepusing(rmb_index) update


display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* removing observations that were already flagged
merge 1:1 FundId using "${dptmp}/toappend_flagged_bloomberg.dta", keepusing(rmb_index) update


display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* checking data is at the fund level

duplicates report FundId

* keeping only: 1) observations that have missing rmb_index or 2) observations that already have jpm flag (to use as comparison for possible candidate indexes)

keep if missing(rmb_index) | ustrregexm(rmb_index, "jpm")
tab rmb_index, m

* handler variables for index names

gen cand_included = ""
gen cand_benchmark = ""

**unifying jpm label**

//- unifying jpm label

cap drop index_fam
display "... family of indexes (all are jp morgan, except 'jpmam' and 'msci')"
qui gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(j\.?\s*p\.?\s*m[a-z0-9]*)") 
qui replace index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "^(.*)") & index_fam == "jp min"
qui replace index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "^(.*)") & index_fam == "jpmam"
tab index_fam

* dismissing non GBI-EM index

replace rmb_index = "other, non relevant" if index_fam == "msci ac ap ex jp min vol (usd) nr usd"
replace rmb_index = "other, non relevant" if index_fam == "jpmam carbon transition gbl eqt usd"
replace index_fam = "" if index_fam == "msci ac ap ex jp min vol (usd) nr usd"
replace index_fam = "" if index_fam == "jpmam carbon transition gbl eqt usd"

* unifying jpm label for relevant indexes

replace Primary_Prospectus_Benchmark = ustrregexra(Primary_Prospectus_Benchmark, "j\.?\s*p\.?\s*m[a-z0-9]*", "jpm") if ! missing(index_fam)
gen index_fam_2 = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(j\.?\s*p\.?\s*m[a-z0-9]*)")
display ".... index_fam_2 is the new label"
tab index_fam index_fam_2

drop index_fam index_fam_2


* creating flag for already filled indexes and possible candidates

cap drop filled
gen filled = 1 if rmb_index == "jpm"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "j.*gbi.*em") & ///
                      missing(rmb_index)

* finding value of funds per candidate index

cap drop val
sort filled Primary_Prospectus_Benchmark
bys filled Primary_Prospectus_Benchmark: egen val = total(FundSize_2021_06)

* list of candidates possible benchmarks and value with respect to total already benchmarked

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) val, by(filled Primary_Prospectus_Benchmark)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
list Primary_Prospectus_Benchmark prop o if o <= 10
* selecting flagged candidate indexes (all look small or do not correspond to our interest index. I dismiss all).
qui levelsof Primary_Prospectus_Benchmark, local(dismissed)
restore

* dismissed

foreach bm of local dismissed {
    qui replace rmb_index = "jpm (dismissed, not checked)" if Primary_Prospectus_Benchmark == "`bm'"
}

* checking selection (look nice)

display "... current selection"
tab Primary_Prospectus_Benchmark if rmb_index == "jpm"

display "... others (dismissed and candidates)"
tab rmb_index_fam if rmb_index != "jpm"

**taking out all "other indexes" indentified , particular non-interest countries**

//- taking out "embi", "cembi" "jpm eur", "jpm us", "jpm euro", "jpm emu"

* flagging 'take out' indexes

cap drop filled
gen filled = 1 if rmb_index == "jpm"
local regex `" "embi" "cembi" "jpm eur" "jpm us" "jpm euro" "jpm emu" "jpm cash" "elmi" "'
foreach re of local regex {
    replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, `"`re'"') & missing(rmb_index)
}

* finding value of funds of  indexes with respect to already flagged indexes

cap drop val
sort filled Primary_Prospectus_Benchmark
bys filled Primary_Prospectus_Benchmark: egen val = total(FundSize_2021_06)

* list of 'take out' indexes
preserve
collapse (first) val ISIN, by(filled Primary_Prospectus_Benchmark)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n

* from 1-70, all are non-wanted indexes (not GBI-EM)
qui levelsof Primary_Prospectus_Benchmark if o <= 70, local(others)

restore

foreach bm of local others {
    qui replace rmb_index = "jpm (other)" if Primary_Prospectus_Benchmark == "`bm'"
}

*drop filled val

**other possible regex for "JPM GBI-EM"**

* creating flag for already filled indexes and possible candidates
cap drop filled
gen filled = 1 if rmb_index == "jpm"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "jpm") & ///
                      missing(rmb_index)

* finding value of funds per candidate index

cap drop val
sort filled Primary_Prospectus_Benchmark
bys filled Primary_Prospectus_Benchmark: egen val = total(FundSize_2021_06)

* list of candidates possible benchmarks

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) val ISIN, by(filled Primary_Prospectus_Benchmark)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot

gen o = _n
list Primary_Prospectus_Benchmark ISIN prop o in 1/20

qui levelsof Primary_Prospectus_Benchmark if o <= 1/20, local(others)
restore

* flagging others

foreach bm of local others {
    qui replace rmb_index = "jpm (other)" if Primary_Prospectus_Benchmark == "`bm'"
}

* identified JPM GBI-EM vs identified to check

cap drop filled
gen filled = 1 if rmb_index == "jpm"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "jpm") & missing(rmb_index)

* general family index

cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(jpm\s*[a-z0-9]+\s*[a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if ! inlist(o, 20) & o <= 70, local(others)
restore

* flag others

foreach bm of local others {
    qui replace rmb_index = "jpm (other)" if index_fam == "`bm'"
}

cap drop h_tocheck 
cap drop index_fam

* checking value of other candidates relative to current selected jpm indexes

preserve
cap drop filled
gen filled = 1 if rmb_index == "jpm"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "jpm") & missing(rmb_index)
collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot
restore

* only 3% of the funds not checked.
replace rmb_index = "jpm (dismissed, not checked)" if ustrregexm(Primary_Prospectus_Benchmark, "jpm") & missing(rmb_index)

* checking all have identifiers (SecId should be the identifier of the fund)

duplicates report FundId

* save matched observations WITH index ID
drop filled
keep if ! missing(rmb_index)
tab rmb_index, m
save "${dptmp}/toappend_flagged_jpm.dta", replace

******************************************************************************************************
* FTSE WGBI Bond Index
******************************************************************************************************

use "${dptmp}/tmp_handydata_to_flag.dta", clear

//- filling values of observations that were already filled in previous steps

merge 1:1 FundId using "${dptmp}/toappend_composite_indexes.dta", keepusing(rmb_index) update

display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* removing observations that were already flagged

merge 1:1 FundId using "${dptmp}/toappend_flagged_bloomberg.dta", keepusing(rmb_index) update


display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* removing observations that were already flagged
merge 1:1 FundId using "${dptmp}/toappend_flagged_jpm.dta", keepusing(rmb_index) update

display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

* checking data is at the fund level

duplicates report FundId

* keeping only: 1) observations that have missing rmb_index or 2) observations that already have jpm flag (to use as comparison for possible candidate indexes)

keep if missing(rmb_index) | ustrregexm(rmb_index, "ftse")
tab rmb_index, m

* handler variables for index names

gen cand_included = ""
gen cand_benchmark = ""

cap drop filled
gen filled = 1 if rmb_index == "ftse"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "ftse") & missing(rmb_index)

* general family index
cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(ftse\s*[a-z0-9]+\s*[a-z0-9]+)") // ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(ftse\s*t\s*[a-z0-9]+\s*[a-z0-9]+)")

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
*gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if o <= 70, local(all)
qui levelsof index_fam if inlist(o, 8, 11, 14, 20, 34, 46, 48, 50, 51, 58), local(check)
qui local others : list all - check
restore

* flag check

gen h_tocheck = .
foreach bm of local check {
    qui replace h_tocheck = 1 if index_fam == "`bm'"
}

* flag others
foreach bm of local others {
    qui replace rmb_index = "ftse (other)" if index_fam == "`bm'"
}

//- Checking the ftse indexes flagged as 'check' 2: using local 'h_tocheck' defined in previous block to check possible indexes

preserve
keep if h_tocheck == 1
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(Primary_Prospectus_Benchmark)
gsort -val
egen tot = total(val)
gen prop = val / tot
gen o = _n

list Primary_Prospectus_Benchmark ISIN prop o

* defining flagged indexes.

qui levelsof Primary_Prospectus_Benchmark, local(allindexes)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 3, 4, 8, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 24, 25, 26, 28, 30), local(included)
qui levelsof Primary_Prospectus_Benchmark if o > 30, local(small)
qui local others : list allindexes - included
qui local others : list others - small
restore

* included

foreach bm of local included {
    qui replace rmb_index_fam = "ftse wgbi" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index = "ftse" if Primary_Prospectus_Benchmark == "`bm'"
}

* dismissed by size

foreach bm of local small {
    qui replace rmb_index = "ftse (dismissed by size)" if Primary_Prospectus_Benchmark == "`bm'"
}

* other indexes

foreach bm of local others {
    qui replace rmb_index = "ftse (other)" if Primary_Prospectus_Benchmark == "`bm'"
}

cap drop h_tocheck 
cap drop index_fam

**Further checking remaining FTSE indexes**

* identified ftse vs to check
cap drop filled
gen filled = 1 if rmb_index == "ftse"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "ftse") & missing(rmb_index)

* general family index

cap drop index_fam
gen index_fam = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "(ftse\s*[a-z0-9]+)") & filled == 0

* list of most important family of indexes

preserve
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(filled index_fam)
gen tosum = cond(filled == 1, 1, 0)
replace tosum = tosum * val
egen tot = total(tosum)
keep if filled == 0
gsort -val
gen prop = val / tot
gen o = _n
* flagging indexes to check and indexes to dismiss
qui levelsof index_fam if o <= 70, local(all)
qui levelsof index_fam if inlist(o, 1, 5), local(check)
qui local others : list all - check
restore

* flag check

gen h_tocheck = .

foreach bm of local check {
    qui replace h_tocheck = 1 if index_fam == "`bm'"
}

* flag others
foreach bm of local others {
    qui replace rmb_index = "ftse (other)" if index_fam == "`bm'"
}

//- Further checking the rest of FTSE indexes 2: Checking remaining indexes with h_tocheck = 1

preserve
keep if h_tocheck == 1
gsort Primary_Prospectus_Benchmark -FundSize_2021_06
collapse (first) ISIN (sum) val = FundSize_2021_06, by(Primary_Prospectus_Benchmark)
gsort -val
egen tot = total(val)
gen prop = val / tot
gen o = _n


* defining flagged indexes.
qui levelsof Primary_Prospectus_Benchmark, local(allindexes)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 1, 2, 7, 9, 10, 13, 15, 16, 17, 18, 19, 21), local(included)
qui levelsof Primary_Prospectus_Benchmark if inlist(o, 3, 11), local(no_idea)
qui levelsof Primary_Prospectus_Benchmark if o >= 22, local(small)
qui local others : list allindexes - included
qui local others : list others - no_idea
qui local others : list others - small
restore

* included
foreach bm of local included {
    qui replace rmb_index_fam = "ftse wgbi" if Primary_Prospectus_Benchmark == "`bm'"
    qui replace rmb_index = "ftse" if Primary_Prospectus_Benchmark == "`bm'"
}

foreach bm of local no_idea {
    qui replace rmb_index = "ftse (dismissed, not checked, prob2)" if Primary_Prospectus_Benchmark == "`bm'"
}

* dismissed by size
foreach bm of local small {
    
    qui replace rmb_index = "ftse (dismissed by size)" if Primary_Prospectus_Benchmark == "`bm'"
    
}

* other indexes
foreach bm of local others {
    qui replace rmb_index = "ftse (other)" if Primary_Prospectus_Benchmark == "`bm'"
    
}

cap drop h_tocheck 
cap drop index_fam

* checking value of other possible candidates relative to current selected ftse indexes

preserve
cap drop filled
gen filled = 1 if rmb_index == "ftse"
replace filled = 0 if ustrregexm(Primary_Prospectus_Benchmark, "ftse") & missing(rmb_index)
collapse (sum) FundSize_2021_06, by(filled)
drop if missing(filled)
egen tot = total(FundSize_2021_06)
gen prop = FundSize_2021_06 / tot
restore

* only 4% of the funds not checked.

replace rmb_index = "ftse (dismissed, not checked)" if ustrregexm(Primary_Prospectus_Benchmark, "ftse") & missing(rmb_index)

* checking all have identifiers (SecId should be the identifier of the fund)

duplicates report FundId

* save matched observations WITH index ID
drop filled
keep if ! missing(rmb_index)
tab rmb_index, m
save "${dptmp}/toappend_flagged_ftse.dta", replace

******************************************************************************************************
* MERGING ALL
******************************************************************************************************


* empty/non-flagged handy data
use "${dptmp}/tmp_handydata_to_flag.dta", clear
gen cand_benchmark = ""
gen cand_included = ""

//- filling values of observations that were already filled in previous steps

merge 1:1 FundId using "${dptmp}/toappend_composite_indexes.dta", keepusing(rmb_index) update

display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

//- filling values of observations that were already filled with bloomberg global aggregate

merge 1:1 FundId using "${dptmp}/toappend_flagged_bloomberg.dta", keepusing(rmb_index rmb_index_fam cand_included cand_benchmark) update


display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

//- filling values of observations that were already filled with jpm gbi em index

merge 1:1 FundId using "${dptmp}/toappend_flagged_jpm.dta", keepusing(rmb_index rmb_index_fam cand_included cand_benchmark) update

display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

//- filling values of observations that were already filled with ftse wgbi

merge 1:1 FundId using "${dptmp}/toappend_flagged_ftse.dta", keepusing(rmb_index rmb_index_fam cand_included cand_benchmark) update


display ". rmb_index for _merge == 3"
tab rmb_index if _merge == 3, m
display ". rmb_index for _merge == 4"
tab rmb_index if _merge == 4, m
drop _merge

keep FundId rmb_index rmb_index_fam Primary_Prospectus_Benchmark cand_included cand_benchmark


* recall FundId is the identifier of the fund.
duplicates report FundId
rename (Primary_Prospectus_Benchmark) (benchmk_lowcase)
save "${dptmp}/tmp_data.dta", replace

******************************************************************************************************
* Merging Fuzzy-Matched index names with main data of funds (2017-2020)
******************************************************************************************************

use "${dptmp}/tmp_fundSize_fundLevel.dta", clear

* merging flags and organizing
merge 1:1 FundId using "${dptmp}/tmp_data.dta"
order benchmk_lowcase, after(Primary_Prospectus_Benchmark)
replace cand_benchmark = Primary_Prospectus_Benchmark if ! missing(cand_benchmark)
replace cand_included = Primary_Prospectus_Benchmark if ! missing(cand_included)
drop _merge

* save
save "${dptmp}/tmp_data_before_fullmapping.dta", replace

******************************************************************************************************
* Adding Extra Variables from Full Mapping Data
******************************************************************************************************

use "${gcap_data}/output/morningstar/output/Morningstar_Mapping_Build/boothmapping_full.dta", clear

* database at the security level (InvestmentProductID). I think InvestmentProductId is the same SecId comming from Morningstar, but as I do not need to use this variable for now, I don't check this.
duplicates report InvestmentProductId

* within fund, all securities should have same domicile. (ok)
bys FundId DomicileCountryId : gen ndomi = _n == 1
bys FundId : replace ndomi = sum(ndomi)
bys FundId : replace ndomi = ndomi[_N]
tab ndomi // almost all. Will ignore cases where it does not 
drop ndomi

* keeping domicile at the fund level
gsort FundId -DomicileCountryId
collapse (first) DomicileCountryId, by(FundId)
mdesc DomicileCountryId

* renaming domicile
rename DomicileCountryId fund_dom

* merging with cleaned securities flows & size data - _merge == 1 are those funds for which we do not have securities in our data (can be dismissed)
merge 1:1 FundId using "${dptmp}/tmp_data_before_fullmapping.dta"
drop if _merge == 1
drop _merge

* checking we have domicile information for our interest indexes
mdesc fund_dom if ustrregexm(rmb_index, "jpm") | ustrregexm(rmb_index, "bloomberg") | ustrregexm(rmb_index, "ftse")
save "${dptmp}/morningstar_dm9_flows_consolidated.dta", replace

******************************************************************************************************
* Adding Final Family of Index Description
******************************************************************************************************

use "${dptmp}/morningstar_dm9_flows_consolidated.dta", clear

tab rmb_index if ustrregexm(rmb_index, "bloomberg")
tab rmb_index if ustrregexm(rmb_index, "jpm")
tab rmb_index if ustrregexm(rmb_index, "ftse")

* looking interest funds
tab rmb_index if rmb_index == "jpm" | rmb_index == "bloomberg" | rmb_index == "ftse" | ustrregexm(rmb_index, "(candidate)")

* creating empty check variables

gen old_rmb_index = ""
gen flag_index_isin = ""
gen documents_folder = ""
gen flag_description = ""
gen flag_url = ""
gen flag_decision = ""
gen flag_lastcheck = ""

* merging previous decision rules

merge 1:1 FundId using "${dptmp}/tmp_1.dta", keepusing(old_rmb_index flag_index_isin documents_folder flag_description flag_url flag_decision) update
rename _merge _merge1
merge 1:1 FundId using "${dptmp}/tmp_2.dta", keepusing(old_rmb_index flag_index_isin documents_folder flag_description flag_url flag_decision) update
rename _merge _merge2
merge 1:1 FundId using "${dptmp}/tmp_3.dta", keepusing(old_rmb_index flag_index_isin documents_folder flag_description flag_url flag_decision) update
rename _merge _merge3
merge 1:1 FundId using "${dptmp}/tmp_4.dta", keepusing(old_rmb_index flag_index_isin documents_folder flag_description flag_url flag_decision) update
rename _merge _merge4

* manual_checked identifies all observations that appeared in previous manual checks and for which a decision was taken
if _merge1 == 2 | _merge2 == 2 | _merge3 == 2 | _merge4 == 2 error

gen manual_checked = 1 if (_merge1 > 2 | _merge2 > 2 | _merge3 > 2 | _merge4 > 2) & ! missing(flag_decision)
tab manual_checked, m
tab rmb_index, m

gen tmp_handler = rmb_index if manual_checked == 1 | rmb_index == "bloomberg" | rmb_index == "ftse" | rmb_index == "jpm" | ustrregexm(rmb_index, "(candidate)")
drop if missing(tmp_handler)
replace tmp_handler = "bloomberg" if ustrregexm(rmb_index, "bloomberg") & ! ustrregexm(rmb_index, "(candidate)")
replace tmp_handler = "ftse" if ustrregexm(rmb_index, "ftse") & ! ustrregexm(rmb_index, "(candidate)")
replace tmp_handler = "jpm" if ustrregexm(rmb_index, "jpm") & ! ustrregexm(rmb_index, "(candidate)")

* keeping largest fund for each index & index total value

gsort rmb_index FundType -FundSize_2021_06
collapse (first) FundId Name ISIN FundType fund_dom FundSize_2021_06 ///
                 flag_index_isin documents_folder flag_description flag_url flag_decision ///
         (sum) total_index = FundSize_2021_06, ///
         by(Primary_Prospectus_Benchmark tmp_handler rmb_index_fam)

gsort rmb_index -total_index

* finding share of each individual index in rmb_index

bys tmp_handler: egen tot = total(total_index)
gen p_index = 100 * total_index / tot
gen fund_bil = FundSize_2021_06 / 10^9
gen index_bil = total_index / 10^9
gsort tmp_handler -p_index

* keeping interest variables

keep tmp_handler rmb_index_fam Primary_Prospectus_Benchmark index_bil p_index FundId Name ISIN FundType fund_dom fund_bil ///
     flag_index_isin documents_folder flag_description flag_url flag_decision
order tmp_handler rmb_index_fam Primary_Prospectus_Benchmark index_bil p_index FundId Name ISIN FundType fund_dom fund_bil ///
     flag_index_isin documents_folder flag_description flag_url flag_decision

* save before check

save "${dptmp}/tmp_data.dta", replace

//- export to excel for manual check

levelsof tmp_handler, local(indexes)
foreach index of local indexes {
    
    use "${dptmp}/tmp_data.dta", clear
    
    display "`index'"
    
    preserve
    
    keep if tmp_handler == "`index'"
    
    gsort -index_bil
    
    * manual check excel
    
    gen cumsum = sum(p_index)
    
    * [SFO20211215]: current version of the manual check fully integrated into the analysis (manual_check_v3).
    export excel using "${dptmp}/tmp_check_`index'", replace firstrow(variables)
    
    restore
    
}

//- if there are no candidate indexes in current version of manual check, create empty files with no candidates (will be useful when updating batches of manual checks)

levelsof tmp_handler, local(indexes)
foreach index of local indexes {
    
    preserve
    
    use "${dptmp}/tmp_data.dta", clear
    
    qui keep if ustrregexm(tmp_handler, "`index'")
    qui sum if ustrregexm(tmp_handler, "candidate")
    
    if `r(N)' == 0 {
        
        qui drop in 2/`c(N)'
        
        foreach var of varlist _all {
            
            cap replace `var' = .
            cap replace `var' = ""
            
        }
        
        replace tmp_handler = "no candidates"
        
        * [SFO20211215]: current version of the manual check fully integrated into the analysis (manual_check_v3).
        export excel using "${dptmp}/tmp_check_`index' (candidate)", replace firstrow(variables)
        
    }
    
    restore
    
}

******************************************************************************************************
* Mapping
******************************************************************************************************

* keeping master portfolio data at the FundId level

use "${gcap_data}/output/morningstar/output/Morningstar_Mapping_Build/boothmapping_full.dta", clear
duplicates drop FundId, force
des Master, fullnames
rename Master MasterPortfolioId
keep FundId MasterPortfolioId
save "${dptmp}/boothmapping_full_unique.dta", replace


//- merging MasterPortfolioId with benchmarks
use "${dptmp}/morningstar_dm9_flows_consolidated.dta", clear
merge 1:1 FundId using "${dptmp}/boothmapping_full_unique.dta"

* dropping observations without master portfolio ID
drop if missing(MasterPortfolioId)
duplicates drop MasterPortfolioId, force
drop _merge

* how many MasterPortfolioId per FundId?
bys MasterPortfolioId FundId : gen diff = _n == 1
bys MasterPortfolioId FundId : replace diff = sum(diff)
bys MasterPortfolioId FundId : replace diff = diff[_N]
tab diff, m // each MasterPortfolioId is associated with 1 and only 1 fund name. Makes total sense.
drop diff

* drop observations without MasterPortfolioId
keep MasterPortfolioId Primary_Prospectus_Benchmark
save "${dptmp}/MasterPortfolio_with_bm.dta", replace


******************************************************************************************************
* Merging benchmarks with yearly HD files
******************************************************************************************************

* merging benchmarks with yearly HD files
use "${gcap_data}/output/morningstar/output/HD_for_analysis/appended_y.dta", clear
keep if asset_class1=="Bond"
merge m:1 MasterPortfolioId using "${dptmp}/MasterPortfolio_with_bm.dta"

* drop observations that are not in HD files
drop if _merge == 2
drop _merge
rename FundName fund_name

* master portfolio ID should be the identifier of the fund
bys MasterPortfolioId fund_name : gen nnames = _n == 1
bys MasterPortfolioId fund_name : replace nnames = sum(nnames)
bys MasterPortfolioId fund_name : replace nnames = nnames[_N]
tab nnames, m // each MasterPortfolioId is associated with 1 and only 1 fund name. Makes total sense.
drop nnames
rename Dom investor
rename cgs_dom residency

* How many residencies are there by investor?
preserve
drop if investor == "" | residency == ""
bys investor residency : gen nres = _n == 1
bys investor residency : replace nres = sum(nres)
bys investor residency : replace nres = nres[_N]
tab nres, m // each investor is associated with 1 and only 1 residency.
drop nres
restore

rename date_y year
keep year investor MasterPortfolioId fund_name Primary_Prospectus_Benchmark residency currency marketvalue_usd cusip6 securityname cusip isin iso_country_code

replace investor = "Missing" if investor == ""
replace fund_name = "Missing" if fund_name == ""
replace currency = "Missing" if currency == ""
replace residency = "Missing" if residency == ""
replace currency = "RMB_CHN" if (currency == "CNH" | currency == "CNY") & residency == "CHN"
replace currency = "other" if currency != "RMB_CHN" 
gen holdings_total = marketvalue_usd
gen holdings_rmb_total = cond(currency == "RMB_CHN", marketvalue_usd, 0)
gen holdings_rmb_foreigners = cond(currency == "RMB_CHN" & investor != "CHN", marketvalue_usd, 0)
collapse (firstnm) Primary_Prospectus_Benchmark (sum) holdings_total holdings_rmb_total holdings_rmb_foreigners, by(MasterPortfolioId fund_name year investor)
* reshape: years in columns
reshape wide holdings_total holdings_rmb_total holdings_rmb_foreigners, i(MasterPortfolioId fund_name investor) j(year)
keep fund_name Primary_Prospectus_Benchmark investor holdings_rmb_foreigners* holdings_rmb_total2020 holdings_total2020
order fund_name Primary_Prospectus_Benchmark investor holdings_rmb_foreigners* holdings_rmb_total2020 holdings_total2020
gsort -holdings_rmb_foreigners2020
egen tot = total(holdings_rmb_foreigners2020)
gen p2020 = 100 * holdings_rmb_foreigners2020 / tot
drop tot
gen rmb_share_aum = holdings_rmb_total2020 / holdings_total2020
drop holdings_rmb_total2020 holdings_total2020 holdings_rmb_foreigners2014-holdings_rmb_foreigners2016
* saving pre-export (to save time when formatting, data takes long to run)
save "${dptmp}/tmp_pre_export.dta", replace


use "${dptmp}/tmp_pre_export.dta", clear
//- fixing table pre-export

* collapsing per final value
cap gen o = _n
local show = 75
replace o = cond(o > `show', 99, o)
local others = _N - `show'
collapse (first) fund_name investor Primary_Prospectus_Benchmark (sum) holdings_rmb_foreigners* p rmb_share_aum, by(o)
replace fund_name = "Other `others' more" if o == 99
replace investor = "" if o == 99
replace Primary_Prospectus_Benchmark = "" if o == 99
format rmb_share_aum %9.2f
tostring rmb_share_aum, replace force usedisplayformat
replace rmb_share_aum = "" if o == 99
drop o

* final format fixes
* final format fixes
replace Primary_Prospectus_Benchmark = "Composite with Bbg.Glb.Agg." if ustrregexm(Primary_Prospectus_Benchmark, "\(.*?\)") & ustrregexm(Primary_Prospectus_Benchmark, "Bloomberg Global Aggregate")
replace Primary_Prospectus_Benchmark = "Composite with JPM.GBI.EM" if ustrregexm(Primary_Prospectus_Benchmark, "\(.*?\)") & ustrregexm(Primary_Prospectus_Benchmark, "JPM GBI-EM Global Diversified")
replace Primary_Prospectus_Benchmark = "Composite (other)" if ustrregexm(Primary_Prospectus_Benchmark, "\(.*?\)")

//- adding total row

cap drop o
qui des, varlist
qui local varlist `r(varlist)'
cap gen o = _n

qui save "${dptmp}/tmp_data.dta", replace
qui clear

input `varlist'
. . . . . . . . .
end

qui tostring fund_name investor Primary_Prospectus_Benchmark rmb_share_aum, force replace
qui gen o = 99
qui append using "${dptmp}/tmp_data.dta"
qui sort o

//- adding total values

qui replace fund_name = "\textbf{Total}" if o == 99

foreach var of varlist holdings_rmb_foreigners2020-p2020 {
    
    qui egen tmp = total(`var')
    qui replace `var' = tmp if o == 99
    qui drop tmp
    
}

* final format fix

format %9.3f holdings_rmb_foreigners2017-holdings_rmb_foreigners2020
format %9.3f holdings_rmb_foreigners2020
format %9.2f p2020
replace investor = "" if o == 99
replace Primary_Prospectus_Benchmark = "" if o == 99
drop o

* manually adding benchmark for Taiwan Funds
replace Primary_Prospectus_Benchmark = "BBgBarc CHN Policy Bk 5+ Y 15 Bln TR CNY" if regexm(fund_name, "Fuh Hwa China 5\+ Yr Policy Bank Bond ETF")
replace Primary_Prospectus_Benchmark = "FTSE Chinese Policy Bank Bd 5+ Yr PR CNY" if regexm(fund_name, "Cathay FTSE Chinese Policy Bk Bd5\+YrsETF")
replace Primary_Prospectus_Benchmark = "BBgBarc China Policy Bk 3-10Y TR TWD" if regexm(fund_name, "KGI China Policy Bank 3-10 Year Bond ETF")
replace Primary_Prospectus_Benchmark = "BBgBarc China Policy Bank TR CNY" if regexm(fund_name, "Fubon China Policy Bank Bond ETF")
replace Primary_Prospectus_Benchmark = "ChinaBond 10y Trsy&Plcy Bd Gr Enh TR TWD" if regexm(fund_name, "Shin Kong 10-Y China Trs Plc Bak Grn ETF")
replace Primary_Prospectus_Benchmark = "ICE 7+ Y LC China Plcy Bank Cstd TR CNY" if regexm(fund_name, "CAPITAL ICE 7\+Y CHINA PLCY BK ETF")
replace Primary_Prospectus_Benchmark = "Not Benchmarked" if regexm(fund_name, "HSBC All China Bond Fund")
replace Primary_Prospectus_Benchmark = "BBgBarc Global Agg Ex-Securzed Heg USD" if regexm(fund_name, "JPMorgan Global Bond Fund")

* shortening benchmark names
replace Primary_Prospectus_Benchmark = ustrregexs(1) if ustrregexm(Primary_Prospectus_Benchmark, "^([^\s]*\s[^\s]*\s[^\s]*)") 

* fixing formats before final output
replace Primary_Prospectus_Benchmark = ustrregexra(Primary_Prospectus_Benchmark, "\\$","\\\\$")
replace Primary_Prospectus_Benchmark = ustrregexra(Primary_Prospectus_Benchmark, "&", "\\&")
replace Primary_Prospectus_Benchmark = ustrregexra(Primary_Prospectus_Benchmark, "%", "\\%")
replace fund_name = ustrregexra(fund_name, "\\$","\\\\$")
replace fund_name = ustrregexra(fund_name, "&", "\\&")
replace fund_name = ustrregexra(fund_name, "%", "\\%")

* save for table
save "${gcap_data}/output/index_inclusion/morningstar_top_funds.dta", replace

******************************************************************************************************
* Index Inclusion Analysis
******************************************************************************************************

use "${dptmp}/morningstar_dm9_flows_consolidated.dta", clear
mmerge FundId using "${dptmp}/boothmapping_full_unique.dta"
drop if _merge==2
drop _merge
cap rename Master MasterPortfolioId
save "${dptmp}/api_merged_.dta", replace

use  "${dptmp}/api_merged_.dta", clear
keep if  rmb_index == "jpm" | rmb_index == "bloomberg" | rmb_index == "ftse"
bysort Master: egen count=count(Master)
drop if Master==.
duplicates drop Master, force
save "${dptmp}/api_merged_unique_.dta", replace

forvalues y=2014/2020 {
    display `y'
    qui use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`y'_q_for_analysis.dta", clear
    cap qui drop _merge
    qui merge m:1 MasterPortfolioId using "${dptmp}/api_merged_unique_.dta", keepusing(rmb_index ISIN Index_Fund Primary_Prospectus_Benchmark)
    cap rename ISIN isin_fund
    qui keep if _merge==3
    qui save "${dptmp}/rmb_indices_`y'_q.dta", replace
}

//- appending quarterly HD files
use "${dptmp}/rmb_indices_2014_q.dta"

forvalues y=2015/2020 {
    display `y'
    qui append using "${dptmp}/rmb_indices_`y'_q.dta"
} 

save "${gcap_data}/output/index_inclusion/rmb_indices_appended_q.dta", replace


* Appendix Figure A.XI: Index Inclusion

use "${gcap_data}/output/index_inclusion/rmb_indices_appended_q.dta", clear

//- preparing data
local time = "q"
order currency marketvalue_usd isin cusip securityname date
keep if asset_class1=="Bond"

order currency  marketvalue_usd isin cusip externalid securityname date
replace currency="MISSING" if currency==""
replace currency = "RMB" if currency == "CNH" | currency == "CNY"

gen q=qofd(date)
format q %tq

//- collapsing at the year-index-currency level
collapse (sum) marketvalue_usd, by(q currency rmb_index)
encode currency, gen(cid)
reshape wide market, i(cid curr `time') j(rmb_index) str
rename marketvalue_usd* mvusd_*
replace mvusd_b=0 if mvusd_b==.
replace mvusd_f=0 if mvusd_f==.
replace mvusd_j=0 if mvusd_j==.
tsset cid `time'
tsfill, full
by cid (`time'): carryforward currency, replace
gen int neg`time' = -`time'
sort cid neg`time'
by cid (neg`time'): carryforward currency, replace
replace mvusd_b=0 if mvusd_b==.
replace mvusd_f=0 if mvusd_f==.
replace mvusd_j=0 if mvusd_j==.
bysort `time': egen total_bloomberg=sum(mvusd_b)
bysort `time': egen total_ftse=sum(mvusd_f)
bysort `time': egen total_jpm=sum(mvusd_j)

gen share_bloomberg=mvusd_bloomberg/total_bloomberg
gen share_ftse=mvusd_ftse/total_bloomberg
gen share_jpm=mvusd_jpm/total_jpm

drop neg`time'

drop if `time' <= `=quarterly("2014q4", "YQ")'

twoway (connected mvusd_b `time' if curr == "RMB") ///
       (connected mvusd_j `time' if curr == "RMB") ///
       (connected mvusd_f `time' if curr == "RMB"), ///
       xline(`=quarterly("2019q1", "YQ")', lpattern("dash") lcolor("gs9")) ///
       xline(`=quarterly("2020q1", "YQ")', lpattern("dash") lcolor("gs9")) ///
       graphregion(color("white")) ///
       ytitle("USD Billions") xtitle("") xlabel(220(4)242) ///
       legend(label(1 "Bloomberg Glb. Agg.") label(2 "JPM GBI-EM") label(3 "FTSE WGBI")) ///

graph export "${gcap_data}/output/appendix_figures/A_XIII_benchmarking_RMB_microdataHdgs.eps", replace

* Table A.IV: Biggest Onshore RMB positions of Foreigners in Morningstar
use "${gcap_data}/output/index_inclusion/morningstar_top_funds.dta", clear

keep in 1/25
listtex using "${gcap_data}/output/appendix_figures/table_A_IV_stacked_morningstar_decomposition.tex", replace ///
            rstyle(tabular) ///
            head("\begin{tabular}{l c l c c c c c c}" ///
                 "\hline \hline \\" ///
                 " & & & \multicolumn{4}{c}{RMB holdings (Bn USD)} & & \\" ///
                 "\cline{4-7}" ///
                 "Fund & Dom. & Benchmark (short) & 2017 & 2018 & 2019 & 2020 & \shortstack{Total \% \\ (2020)} & \shortstack{(RMB hdgs) / \\ (Total AUM), \\  2020} \\ \midrule") ///
            foot("\midrule" ///
                 "\end{tabular}")


cap log close