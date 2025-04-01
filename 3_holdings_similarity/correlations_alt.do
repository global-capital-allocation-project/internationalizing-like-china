******************************************************************************************************
* SETUP
******************************************************************************************************

qui do Project_globals.do

* creating the directory to store the results: 
cap mkdir "${gcap_data}/output/holdings_similarity"

* Logs
cap log close
cap mkdir "$gcap_data/rmb_replication/logs"
log using "$gcap_data/rmb_replication/logs/hldgs_similarity_`1'.log", replace

******************************************************************************************************
* CALCULATING HOLDINGS SIMILARITY
******************************************************************************************************

*Note: Other than the alternate definitions, this code is identical to correlations.do

local item=`1'
local run_type "ust weighted excl_index intensive_mg alt_thr alt_aum alt_fc"
local alt `: word `item' of `run_type''
local year "2020"

* defining local for cuts:
local min_amount=1
* specialist
if ("`alt'" != "alt_thr") {
    local spec=0.5
}
else {
    local spec=0.98
}

*minimum AUM
if ("`alt'" != "alt_aum") {
    local aum_cut=0.020
}
else {
    local aum_cut=0.010
}


* read file
use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", clear
cap rename date_y year

* assets with wrong currency in holdings files
cap drop if currency == "BBB-" 
* Classify Policy Bank bonds issuances originally classified as corporates as sovereign (very few)
cap replace asset_class2 = "Sovereign Bond" if regexm(lower(securityname),"china developm") & currency == "CNY" & asset_class2 == "Corporate Bond"

* defining domestic as the base currency of the fund in alternative definition
if ("`alt'"!="alt_fc"){
    mmerge Domicile using "${gcap_data}/input/miscellaneous/country_currency.dta", umatch(iso_country_code) uname("lc_") unmatched(m)
    drop if missing(lc_iso_currency_code)  
    order lc_ iso* curr
    drop if missing(currency)
    drop if missing(iso_country_code)
    gen domestic=1
    replace domestic=0 if lc_~=currency
} 
else {
    cap drop domestic
    rename iso_currency_code fund_cur
    gen domestic = 0 
    replace domestic = 1 if currency == fund_cur
    keep if domestic == 0
    drop fund_cur
}

* Consider CNH AND CNY as a single currency (CNY)
cap gen currency_original = currency
cap replace currency="CNY" if currency=="CNH"
cap replace lc_iso_currency_code="CNY" if lc_iso_currency_code=="CNH"
cap drop currency_original

* keeping only variables of interest
keep MasterPortfolioId DomicileCountryId FundName asset_class* currency cgs_dom country_bg domestic marketvalue_usd year
drop if marketvalue_usd <= 0 | marketvalue_usd == .
cap drop _temp

* compute total AUM of the fund
bys Master year: egen AUM = total(marketvalue_usd)
drop if AUM <=0

* keep LC Government Debt
drop if asset_class2 != "Sovereign Bond"
cap drop iso_currency_code
mmerge country_bg using "${gcap_data}/input/miscellaneous/country_currency.dta", unmatched(m) umatch(iso_country_code)
rename iso_currency_code currency_country
keep if currency_country == currency

* compute the Asset Share of Total AUM: 
bys Master: egen AUM_asset = total(marketvalue_usd)
qui gen share_asset = AUM_asset / AUM
qui drop if missing(share_asset)
qui drop if AUM_asset == 0

* keep Foreign Currency only: 
keep if domestic == 0

* compute the FC AUM of Total Asset: 
by Master: egen AUM_fc = total(marketvalue_usd)
gen share_fc = AUM_fc / AUM_asset
drop if AUM_fc == 0 
drop if AUM_fc < `aum_cut' //(at least 20mi USD in FC holdings of that type of asset: Bond, Govt, LC Govt)

* drop if index fund
if ("`alt'" == "excl_index") {
    mmerge MasterPortfolioId using "${gcap_data}/input/morningstar/morningstar_api_data/ms_index_funds.dta", unmatched(master)
    drop if Index_Fund == "Yes"
    drop _merge
}

* classify countries/currencies:
if ("`alt'" != "ust") {
    qui gen group_currency = "DM" if inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2)
}
else {
    qui gen group_currency = "DM" if currency=="USD"
}


* Collapse to fund-currency-year level
drop AUM_fc
qui collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year AUM* currency group_currency AUM_asset Dom share_asset share_fc )
bys MasterPortfolioId: egen AUM_fc = total(marketvalue_usd)
drop if AUM_fc==0

* fillin the currencies for the full panel:  
drop if missing(Master)
fillin Master currency
replace year = `year' if missing(year)
replace marketvalue_usd = 0 if missing(marketvalue_usd)

* Add fund level information to fund-currency-year observations that were filled in
foreach var of varlist FundName DomicileCountryId AUM AUM_asset share_asset share_fc AUM_fc {
   gsort Master -marketvalue_usd
   by Master: replace `var' = `var'[1] if missing(`var') 
}
drop group_currency

* drop domestic zeros we created by doing the fillin
qui mmerge DomicileCountryId using "${gcap_data}/input/miscellaneous/country_currency.dta", unmatched(m) umatch(iso_country_code)
drop if currency==iso_currency_code
drop iso_currency_code

* classify countries/currencies:    
if ("`alt'" != "ust") {
    qui gen group_currency = "DM" if inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2)
}
else {
    qui gen group_currency = "DM" if currency=="USD"
}

* aux variables to compute the correlations: 
bys Master year: egen _temp_dm = total(market) if group_currency=="DM"
bys Master year: egen dmtotal = max(_temp_dm)
qui drop _temp*
qui gen dmshare = dmtotal / AUM_fc
qui gen share=marketvalue_usd/AUM_fc
* if missing then 0: 
 qui replace dmshare = 0 if missing(dmshare)
* computing extotal for denominator: 
levelsof currency, local(curr)
qui gen excurr_fundtotal=.
    quietly {
    foreach x of local curr {
        gen `x'_temp=marketvalue_usd if currency=="`x'"
        bysort Master year: egen `x'=max(`x'_temp)
        replace `x'=0 if `x'==.
        replace excurr_fundtotal=AUM_fc-`x' if currency=="`x'"
     }
     }
drop *_temp
gen _temp_dm = dmshare
assert dmshare <=1

* calculate the developed market share if we exclude currency/country x
replace dmshare=dmtot/excurr_fundtotal  
quietly {
    foreach x of local curr {    
        replace dmshare=(dmtot-marketvalue_usd)/(excurr_fundtotal) if currency=="`x'" & group_currency=="DM"
    }
}
replace dmshare=. if dmshare<0 // replace negative observations as missing, then make missing=0
replace dmshare =0 if missing(dmshare)

* to store the correlations: 
gen corr_dm_nospec=.

* dropping the specialists
bys MasterPortfolioId: egen max_share = max(share)
drop if max_share >= `spec'

* intensive margin:
if ("`alt'" == "intensive_mg") {
    replace dmshare =0 if abs(dmshare) < 0.001
    keep if share != 0
}

* keeping only countries abpve threshold
qui mmerge currency using "${gcap_data}/output/morningstar/temp/countries_list.dta", umatch(currency) ukeep(currency) unmatched(m)
keep if _merge==3
drop _merge

* save file at fund level
save "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_`alt'.dta", replace

* computing the correlations:
levelsof currency, local(curr)
foreach x of local curr {
cap {
    if ("`alt'" != "weighted") {
        corr share dmshare if currency=="`x'" & share<`spec'
        replace corr_dm_nospec=r(rho) if currency=="`x'"  
    }
    else {
        corr share dmshare [aweight = AUM_fc] if currency=="`x'" & share<`spec'
        replace corr_dm_nospec=r(rho) if currency=="`x'"  
    }
}
}

if ("`alt'" != "weighted") {
    * computing the confidence intervals for the correlations
    levelsof currency, local(curr)
    gen interval_lower=.
    gen interval_upper=.
    gen se=.
    foreach x of local curr {
        cap{
        qui bootstrap r(rho), nodots nowarn reps(10000) seed(2803): corr share dmshare if currency  =="`x'"
        matrix E = r(table)
        matrix list r(table)
        qui replace se = E[2,1] if currency  =="`x'"
        qui replace interval_lower = E[5,1] if currency  =="`x'" 
        qui replace interval_upper = E[6,1] if currency  =="`x'"
        }
    }

    keep currency corr_dm_nospec year interval_lower interval_upper
    duplicates drop
    gsort -corr_dm_nospec
}
else {
    keep currency corr_dm_nospec year 
    duplicates drop
    gsort -corr_dm_nospec
}

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_`alt'.dta", replace

cap log close