******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/offshore_estimate.log", replace

* Install required packages
ssc install kountry

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/foreign_holdings"
cap mkdir "${gcap_data}/output/foreign_holdings/temp"

******************************************************************************************************
* OFFSHORE ANALYSIS
******************************************************************************************************

* Define as domestic and offshore assets (using FIGI) from all assets in CNY and CNH
use "${gcap_data}/input/gcap/gcap_security_master.dta" if curr=="CNY" | curr=="CNH", clear
keep if figi~=""
qui mmerge figi using "${gcap_data}/input/figi/figi_master_compact.dta"
keep if _merge==3
save "${gcap_data}/output/foreign_holdings/temp/cny_figi.dta", replace
* foreign when FIGI classifies as EURO, GLOBAL or currency is offshore RMB (CNH)
gen domestic=1
replace domestic=0 if regexm(securitytype,"EURO")==1 | regexm(securitytype,"GLOBAL")==1 | curr=="CNH"
collapse (firstnm) domestic securitytype, by(cusip)
save "${gcap_data}/output/foreign_holdings/temp/domestic_static.dta", replace

* Using above classification for assets to bond holdings in Morningstar
clear
forval file=2014(1)2020 {
    append using "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`file'_y_for_analysis.dta"
}
save "${gcap_data}/output/morningstar/output/HD_for_analysis/appended_y.dta", replace

cap drop cgs_domicile_source
cap drop currency_original
cap rename date_y year
keep if (curr=="CNY" | curr=="CNH") & (Dom~="CHN" & Dom~="HKG") & asset_class1=="Bond"
cap drop domestic
mmerge cusip using "${gcap_data}/output/foreign_holdings/temp/domestic_static.dta", ukeep(domestic)
drop if _merge==2
replace dom=0 if cgs_dom~="CHN"
collapse (sum) marketvalue_usd, by(domestic Dom year)
save "${gcap_data}/output/foreign_holdings/temp/rmb_domestic.dta", replace

* Computing international/domestic share in holdings
replace dom=2 if dom==.
reshape wide marketvalue_usd, i(Dom year) j(dom)
rename marketvalue_usd0 intl
rename marketvalue_usd1 domestic
rename marketvalue_usd2 missing
label var intl "International"
label var domestic "Domestic"
label var missing "Missing"
replace missing=0 if missing(missing)
replace dom=0 if missing(domestic)
gen agg_domestic=domestic+missing
drop if Dom=="CHN"
collapse (sum) intl agg_dom, by(year)
gen intl_share=intl/(agg_dom+intl)
save "${gcap_data}/output/foreign_holdings/temp/rmb_domestic_aggregated.dta", replace
* saving as long for plots
drop intl_share
rename intl marketvalue_usdintl_rmb
rename agg_domestic marketvalue_usddomestic_rmb
reshape long marketvalue_usd, i(year) j(bond_type) str
save "${gcap_data}/output/foreign_holdings/temp/rmb_domestic_aggregated_long.dta", replace

* Compute forms of bond investment in China and RMB
use "${gcap_data}/output/morningstar/output/HD_for_analysis/appended_y.dta", clear
cap drop cgs_domicile_source
cap drop currency_original
cap rename date_y year
keep if (curr=="CNY" | curr=="CNH" | cgs_dom=="CHN" | country_bg=="CHN") & asset_class1=="Bond"
keep if country_bg=="CHN"
drop if Dom=="CHN" | Dom=="HKG"
gen country_type=cgs_dom
replace country_type="Tax Haven" if inlist(cgs_dom,$tax_haven_1) |  inlist(cgs_dom,$tax_haven_2) |  inlist(cgs_dom,$tax_haven_3) |  inlist(cgs_dom,$tax_haven_4) |  inlist(cgs_dom,$tax_haven_5) |  inlist(cgs_dom,$tax_haven_6) |  inlist(cgs_dom,$tax_haven_7) | inlist(cgs_dom,$tax_haven_8) 
replace country_type="HKG" if cgs_dom=="HKG"
replace country_type="International" if country_type~="Tax Haven" & country_type~="HKG" & country_type~="CHN"
collapse (sum) marketvalue_usd, by(curr country_type Dom year)
drop if curr==""
save "${gcap_data}/output/foreign_holdings/temp/collapse.dta", replace

cap log close