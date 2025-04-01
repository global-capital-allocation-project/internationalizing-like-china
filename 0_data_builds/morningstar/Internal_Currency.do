* --------------------------------------------------------------------------------------------------
* Internal_Currency
*
* This file constructs a dataset with the modal currency assignments for each security within the
* Morningstar holdings data. We look for modal currency assignments within funds and then across funds.
* --------------------------------------------------------------------------------------------------

*cap log close
*log using "$logs/Internal/${whoami}_Internal_Currency", replace

* Append monthly files and keep only relevant variables
forvalues year=$firstyear/$lastyear {
  display "Appending HoldingDetail_`year'_m_extid_merge.dta ..."
  append using "$temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta", keep(cusip currency_id MasterPortfolioId)
} 

* Find fund-specific modal currency assigned to each CUSIP
drop if cusip=="" | currency_id==""
gen counter = 1 if !missing(currency_id)
bysort cusip currency_id MasterPort: egen currency_fund_count=sum(counter)
collapse (firstnm) currency_fund_count, by(cusip currency_id MasterPort)
bysort cusip MasterPort: egen currency_fund_count_max=max(currency_fund_count)
drop if currency_fund_count<currency_fund_count_max

* Split the equally frequent
gen counter = 1 if !missing(currency_id)
bysort cusip MasterPort: egen currency_fund_count_split=sum(counter)
drop counter
bysort cusip MasterPort: gen randt=runiform()
bysort cusip MasterPort: egen randt_max=max(randt)
drop if currency_fund_count_split>=2 & randt<randt_max
drop randt randt_max

* Find modal currency assigned to each CUSIP across funds
gen counter = 1 if !missing(currency_id)
bysort cusip currency_id: egen currency_count=sum(counter)
collapse (firstnm) currency_count, by(cusip currency_id)
bysort cusip: egen currency_count_max=max(currency_count)
drop if currency_count<currency_count_max

* Split the equally frequent
gen counter = 1 if !missing(currency_id)
bysort cusip: egen currency_count_split=sum(count)
drop counter
bysort cusip: gen randt=runiform()
bysort cusip: egen randt_max=max(randt)
drop if currency_count_split>=2 & randt<randt_max
keep cusip currency_id
save "$temp/Internal/Internal_Currency.dta", replace

*log close
