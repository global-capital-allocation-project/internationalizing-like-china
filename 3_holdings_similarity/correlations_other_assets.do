******************************************************************************************************
* SETUP
******************************************************************************************************

qui do Project_globals.do

* creating the directory to store the results: 
cap mkdir "${gcap_data}/output/holdings_similarity"

* Logs
cap log close
cap mkdir "$gcap_data/rmb_replication/logs"
log using "$gcap_data/rmb_replication/logs/hldgs_similarity_other_assets.log", replace

******************************************************************************************************
* HOLDINGS SIMILARITY EQUITY
******************************************************************************************************

* year to analyze: 
local year=2020

* defining local for cuts:
local spec=0.5
local aum_cut=0.020
local min_amount=1

* year to analyze: 
local year=2020

* defining local for cuts:
local spec=0.5
local aum_cut=0.020
local min_amount=1

* read file
use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", clear
cap rename date_y year

* assets with wrong currency in holdings files
cap drop if currency == "BBB-" 
* Classify Policy Bank bonds issuances originally classified as corporates as sovereign (very few)
cap replace asset_class2 = "Sovereign Bond" if regexm(lower(securityname),"china developm") & currency == "CNY" & asset_class2 == "Corporate Bond"

* defining domestic: by nationality, not currency. country_bg is the nationality of an issuer.
drop if country_bg == DomicileCountryId

* Consider CNH AND CNY as a single currency (CNY)
gen currency_original = currency
replace currency="CNY" if currency=="CNH"
cap replace lc_iso_currency_code="CNY" if lc_iso_currency_code=="CNH"
cap drop currency_original

keep MasterPortfolioId DomicileCountryId FundName asset_class* currency cgs_dom country_bg marketvalue_usd year
drop if marketvalue_usd <= 0 | marketvalue_usd == .
cap drop _temp

* compute total AUM of the fund
bys Master year: egen AUM = total(marketvalue_usd)
drop if AUM <=0

* keep Equities only
keep if asset_class1=="Equity"

* consider EMU as a group
replace country_bg = "EMU" if inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3)
replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,$eu1) | inlist(DomicileCountryId,$eu2) | inlist(DomicileCountryId,$eu3)

* define countries to analyze
preserve
use "${gcap_data}/output/morningstar/temp/countries_list.dta", clear
qui mmerge currency using "${gcap_data}/input/miscellaneous/country_currency.dta", umatch(iso_currency_code) unmatched(m)
drop _merge
save "${gcap_data}/output/morningstar/temp/countries_list_iso_country.dta", replace
restore

* compute the Asset Share of Total AUM: 
bys Master: egen AUM_asset = total(marketvalue_usd)
qui gen share_asset = AUM_asset / AUM
qui drop if missing(share_asset)
qui drop if AUM_asset == 0

* compute the FC AUM of Total Asset: 
by Master: egen AUM_fc = total(marketvalue_usd)
gen share_fc = AUM_fc / AUM_asset
drop if AUM_fc == 0 
drop if AUM_fc < `aum_cut' 

* classify countries/currencies:
gen group_country_bg = "DM" if ( inlist(country_bg,$dmcountry1) | inlist(country_bg,$dmcountry2) | inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU")

* Collapse to fund-currency-year level
drop AUM_fc
qui collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year country_bg year AUM* group_country_bg AUM_asset Dom share_asset share_fc )
bys MasterPortfolioId: egen AUM_fc = total(marketvalue_usd) 
collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year AUM  country_bg group_country_bg AUM_fc AUM_asset Dom share_asset share_fc )
drop if AUM_fc==0

* fillin the currencies for the full panel:  
drop if missing(Master)
fillin Master country_bg
replace year = `year' if missing(year)
replace marketvalue_usd = 0 if missing(marketvalue_usd)

* Add fund level information to fund-currency-year observations that were filled in
foreach var of varlist FundName DomicileCountryId AUM AUM_asset share_asset share_fc AUM_fc {
   gsort Master -marketvalue_usd
   by Master: replace `var' = `var'[1] if missing(`var') 
}
drop group_country_bg

* drop domestic zeros we created by doing the fillin
drop if country_bg==DomicileCountryId

* classify countries/currencies:       
gen group_country_bg = "DM" if ( inlist(country_bg,$dmcountry1) | inlist(country_bg,$dmcountry2) | inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU")

* aux variables to compute the correlations: 
bys Master year: egen _temp_dm = total(market) if group_country_bg=="DM"
bys Master year: egen dmtotal = max(_temp_dm)
qui drop _temp*
qui gen dmshare = dmtotal / AUM_fc
qui gen share=marketvalue_usd/AUM_fc
* if missing then 0: 
 qui replace dmshare = 0 if missing(dmshare)
* computing AUM of foreign equities excluding the particular country being analyzed (extotal) for denominator: 
* Note, we call the local curr to maintain the parallel with correlations.do, but this loops over countries
levelsof country_bg, local(curr)
qui gen excurr_fundtotal=.
    quietly {
    foreach x of local curr {
        gen `x'_temp=marketvalue_usd if country_bg=="`x'"
        bysort Master year: egen `x'=max(`x'_temp)
        replace `x'=0 if `x'==.
        replace excurr_fundtotal=AUM_fc-`x' if country_bg=="`x'"
     }
     }
drop *_temp
gen _temp_dm = dmshare
assert dmshare <=1

* calculate the developed market share if we exclude currency/country x
replace dmshare=dmtot/excurr_fundtotal  
quietly {
    foreach x of local curr {    
        replace dmshare=(dmtot-marketvalue_usd)/(excurr_fundtotal) if country_bg=="`x'" & group_country_bg=="DM"
    }
}

replace dmshare=. if dmshare<0 // replace negative observations as missing, then make missing=0
replace dmshare =0 if missing(dmshare)

* to store the correlations: 
gen corr_dm_nospec=.

* dropping the specialists
bys MasterPortfolioId: egen max_share = max(share)
drop if max_share >= `spec'

* keeping only countries abpve threshold
qui mmerge country_bg using "${gcap_data}/output/morningstar/temp/countries_list_iso_country.dta", umatch(iso_country_code) unmatched(m)

keep if (_merge == 3 |  inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU") 
qui replace country_bg = "EMU" if inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3)
cap drop _merge

* save file at fund level
save "${gcap_data}/output/holdings_similarity/holdings_equity_nationality_`year'.dta", replace

* computing the correlations:
foreach x of local curr {
cap {
    corr share dmshare if country_bg=="`x'" & share<`spec'
    replace corr_dm_nospec=r(rho) if country_bg=="`x'"  
    }
}

* Drop Jersey
cap drop if country_bg=="JEY"

* computing thcountry_bgdence intervals for the correlations
levelsof country_bg, local(curr)
gen interval_lower=.
gen interval_upper=.
gen se=.
foreach x of local curr {
    di "`x'"
    qui bootstrap r(rho), nodots nowarn reps(10000) seed(2803): corr share dmshare if country_bg  =="`x'"
    matrix E = r(table)
    matrix list r(table)
    qui replace se = E[2,1] if country_bg =="`x'"
    qui replace interval_lower = E[5,1] if country_bg  =="`x'" 
    qui replace interval_upper = E[6,1] if country_bg  =="`x'"
}

keep country corr_dm_nospec year interval_lower interval_upper
duplicates drop
gsort -corr_dm_nospec

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_equity_nationality_`year'.dta", replace

******************************************************************************************************
* HOLDINGS SIMILARITY USD BONDS
******************************************************************************************************

* read file
use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", clear
cap rename date_y year

* assets with wrong currency in holdings files
cap drop if currency == "BBB-" 
* Classify Policy Bank bonds issuances originally classified as corporates as sovereign (very few)
cap replace asset_class2 = "Sovereign Bond" if regexm(lower(securityname),"china developm") & currency == "CNY" & asset_class2 == "Corporate Bond"

* defining domestic: by nationality, not currency. country_bg is the nationality of an issuer.
drop if country_bg == DomicileCountryId

* Consider CNH AND CNY as a single currency (CNY)
gen currency_original = currency
replace currency="CNY" if currency=="CNH"
cap replace lc_iso_currency_code="CNY" if lc_iso_currency_code=="CNH"
cap drop currency_original

keep MasterPortfolioId DomicileCountryId FundName asset_class* currency cgs_dom country_bg marketvalue_usd year
drop if marketvalue_usd <= 0 | marketvalue_usd == .
cap drop _temp

* compute total AUM of the fund
bys Master year: egen AUM = total(marketvalue_usd)
drop if AUM <=0

* keep USD Corporate Bonds
keep if currency == "USD"
drop if asset_class2 != "Corporate Bond"

* consider EMU as a group
replace country_bg = "EMU" if inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3)
replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,$eu1) | inlist(DomicileCountryId,$eu2) | inlist(DomicileCountryId,$eu3)

* define countries to analyze
preserve
use "${gcap_data}/output/morningstar/temp/countries_list.dta", clear
qui mmerge currency using "${gcap_data}/input/miscellaneous/country_currency.dta", umatch(iso_currency_code) unmatched(m)
drop _merge
save "${gcap_data}/output/morningstar/temp/countries_list_iso_country.dta", replace
restore

* compute the Asset Share of Total AUM: 
bys Master: egen AUM_asset = total(marketvalue_usd)
qui gen share_asset = AUM_asset / AUM
qui drop if missing(share_asset)
qui drop if AUM_asset == 0

* compute the FC AUM of Total Asset: 
by Master: egen AUM_fc = total(marketvalue_usd)
gen share_fc = AUM_fc / AUM_asset
drop if AUM_fc == 0 
drop if AUM_fc < `aum_cut' 

* classify countries/currencies:
gen group_country_bg = "DM" if ( inlist(country_bg,$dmcountry1) | inlist(country_bg,$dmcountry2) | inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU")

* Collapse to fund-currency-year level
drop AUM_fc
qui collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year country_bg year AUM* group_country_bg AUM_asset Dom share_asset share_fc )
bys MasterPortfolioId: egen AUM_fc = total(marketvalue_usd) 
collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year AUM  country_bg group_country_bg AUM_fc AUM_asset Dom share_asset share_fc )
drop if AUM_fc==0

* fillin the currencies for the full panel:  
drop if missing(Master)
fillin Master country_bg
replace year = `year' if missing(year)
replace marketvalue_usd = 0 if missing(marketvalue_usd)

* Add fund level information to fund-currency-year observations that were filled in
foreach var of varlist FundName DomicileCountryId AUM AUM_asset share_asset share_fc AUM_fc {
   gsort Master -marketvalue_usd
   by Master: replace `var' = `var'[1] if missing(`var') 
}
drop group_country_bg

* drop domestic zeros we created by doing the fillin
drop if country_bg==DomicileCountryId

* classify countries/currencies:       
gen group_country_bg = "DM" if ( inlist(country_bg,$dmcountry1) | inlist(country_bg,$dmcountry2) | inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU")

* aux variables to compute the correlations: 
bys Master year: egen _temp_dm = total(market) if group_country_bg=="DM"
bys Master year: egen dmtotal = max(_temp_dm)
qui drop _temp*
qui gen dmshare = dmtotal / AUM_fc
qui gen share=marketvalue_usd/AUM_fc
* if missing then 0: 
 qui replace dmshare = 0 if missing(dmshare)
* computing AUM of foreign equities excluding the particular country being analyzed (extotal) for denominator: 
* Note, we call the local curr to maintain the parallel with correlations.do, but this loops over countries
levelsof country_bg, local(curr)
qui gen excurr_fundtotal=.
    quietly {
    foreach x of local curr {
        gen `x'_temp=marketvalue_usd if country_bg=="`x'"
        bysort Master year: egen `x'=max(`x'_temp)
        replace `x'=0 if `x'==.
        replace excurr_fundtotal=AUM_fc-`x' if country_bg=="`x'"
     }
     }
drop *_temp
gen _temp_dm = dmshare
assert dmshare <=1

* calculate the developed market share if we exclude currency/country x
replace dmshare=dmtot/excurr_fundtotal  
quietly {
    foreach x of local curr {    
        replace dmshare=(dmtot-marketvalue_usd)/(excurr_fundtotal) if country_bg=="`x'" & group_country_bg=="DM"
    }
}

replace dmshare=. if dmshare<0 // replace negative observations as missing, then make missing=0
replace dmshare =0 if missing(dmshare)

* to store the correlations: 
gen corr_dm_nospec=.

* dropping the specialists
bys MasterPortfolioId: egen max_share = max(share)
drop if max_share >= `spec'

* keeping only countries abpve threshold
qui mmerge country_bg using "${gcap_data}/output/morningstar/temp/countries_list_iso_country.dta", umatch(iso_country_code) unmatched(m)
keep if (_merge == 3 |  inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3) | country_bg=="EMU") 
qui replace country_bg = "EMU" if inlist(country_bg,$eu1) | inlist(country_bg,$eu2) | inlist(country_bg,$eu3)
cap drop _merge

* save file at fund level
save "${gcap_data}/output/holdings_similarity/holdings_usd_bonds_nationality_`year'.dta", replace

* computing the correlations:
foreach x of local curr {
cap {
    corr share dmshare if country_bg=="`x'" & share<`spec'
    replace corr_dm_nospec=r(rho) if country_bg=="`x'"  
    }
}

* Drop Jersey
cap drop if country_bg=="JEY"

* computing thcountry_bgdence intervals for the correlations
levelsof country_bg, local(curr)
gen interval_lower=.
gen interval_upper=.
gen se=.
foreach x of local curr {
    di "`x'"
    qui bootstrap r(rho), nodots nowarn reps(10000) seed(2803): corr share dmshare if country_bg  =="`x'"
    matrix E = r(table)
    matrix list r(table)
    qui replace se = E[2,1] if country_bg =="`x'"
    qui replace interval_lower = E[5,1] if country_bg  =="`x'" 
    qui replace interval_upper = E[6,1] if country_bg  =="`x'"
}

keep country corr_dm_nospec year interval_lower interval_upper
duplicates drop
gsort -corr_dm_nospec

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_usd_bonds_nationality_`year'.dta", replace

cap log close