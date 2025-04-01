******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/table_summary.log", replace

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/paper_figures"
cap mkdir "${gcap_data}/output/appendix_figures"

ssc install listtex
******************************************************************************************************
* TABLE SUMMARY
******************************************************************************************************

* defining local for cuts:
local spec=0.5
local aum_cut=0.020
local min_amount=1
local year "2020"

use  "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_baseline.dta", clear
drop if max_share >= `spec'
replace Dom= "EMU" if inlist(Dom,$eu1) | inlist(Dom,$eu2) | inlist(Dom,$eu3)
keep AUM* MasterPortfolioId DomicileCountryId
duplicates drop

save "${gcap_data}/temp/list_of_funds.dta", replace

* Column 1
preserve
collapse (count) AUM
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (count) AUM, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
rename AUM number_of_funds
gsort -number_of_funds
keep if _n <5
save "${gcap_data}/temp/table_col1.dta", replace
restore

* Column 2
preserve
collapse (mean) AUM
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (mean) AUM, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
rename AUM AUMmean
replace AUMmean = AUMmean*1000
save "${gcap_data}/temp/table_col2.dta", replace
restore

* Column 3
preserve
collapse (median) AUM
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (median) AUM, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
rename AUM AUMmedian
replace AUMmedian = AUMmedian*1000
save "${gcap_data}/temp/table_col3.dta", replace
restore

* Column 4
preserve
collapse (mean) AUM_fc
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (mean) AUM_fc, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
rename AUM AUM_fcmean
replace AUM_fcmean = AUM_fcmean*1000
save "${gcap_data}/temp/table_col4.dta", replace
restore

* Column 5
preserve
collapse (median) AUM_fc
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (median) AUM_fc, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
rename AUM AUM_fcmedian
replace AUM_fcmedian = AUM_fcmedian*1000
save "${gcap_data}/temp/table_col5.dta", replace
restore

local year "2020"
* read file
use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", clear
cap rename date_y year

* assets with wrong currency in holdings files
cap drop if currency == "BBB-" 
* adjustment for (very) few Development Bank issuances classified as corporates
cap replace asset_class2 = "Sovereign Bond" if regexm(lower(securityname),"china developm") & currency == "CNY" & asset_class2 == "Corporate Bond"

* defining domestic 
mmerge Domicile using "${gcap_data}/input/miscellaneous/country_currency.dta", umatch(iso_country_code) uname("lc_") unmatched(m)
drop if missing(lc_iso_currency_code)  
order lc_ iso* curr
drop if missing(currency)
drop if missing(iso_country_code)
gen domestic=1
replace domestic=0 if lc_~=currency

* consider CNH AND CNY jointly
gen currency_original = currency
replace currency="CNY" if currency=="CNH"
replace lc_iso_currency_code="CNY" if lc_iso_currency_code=="CNH"
cap drop currency_original

keep MasterPortfolioId DomicileCountryId FundName asset_class* currency cgs_dom country_bg domestic marketvalue_usd year
drop if marketvalue_usd <= 0 | marketvalue_usd == .
cap drop _temp

* keeping only funds used in the analysis
qui mmerge Master using "${gcap_data}/temp/list_of_funds.dta", ukeep(Master)
keep if _merge==3

* compute total AUM of the fund
bys Master year: egen AUM = total(marketvalue_usd)
drop if AUM <=0

* keep Foreign Currency only: 
keep if domestic == 0

* compute the FC AUM TOTAL: 
by Master: egen AUM_fc = total(marketvalue_usd)

* keep LC Government Debt
drop if asset_class2 != "Sovereign Bond"
cap drop iso_currency_code
mmerge country_bg using "${gcap_data}/input/miscellaneous/country_currency.dta", unmatched(m) umatch(iso_country_code)
rename iso_currency_code currency_country
keep if currency_country == currency

* compute the Asset Share of Total AUM: 
bys Master: egen AUM_asset = total(marketvalue_usd)

* computing avg share of FC assets
keep Master Dom AUM*
duplicates drop

qui gen share_asset_out_of_FC = AUM_asset / AUM_fc
gen share_fc_out_of_total = AUM_fc / AUM

replace Dom= "EMU" if inlist(Dom,$eu1) | inlist(Dom,$eu2) | inlist(Dom,$eu3)

* Column 6
preserve
collapse (mean) share_fc_out_of_total
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (mean) share_fc_out_of_total, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
save "${gcap_data}/temp/table_col6.dta", replace
restore

* Column 7
preserve
collapse (mean) share_asset_out_of_FC
gen DomicileCountryId = "Funds"
save "${gcap_data}/temp/temp_total_row.dta", replace
restore
preserve
collapse (mean) share_asset_out_of_FC, by(DomicileCountryId)
append using "${gcap_data}/temp/temp_total_row.dta"
save "${gcap_data}/temp/table_col7.dta", replace
restore

* Table A.I: Fund Sample Summary Statistics: 2020

clear 
use "${gcap_data}/temp/table_col1.dta"

forval i=2(1)7 {
    qui mmerge Dom using "${gcap_data}/temp/table_col`i'.dta", unmatched(m)
}
drop _merge
gsort -number_of_funds
gen share_fc_out_of_total_str = string(100 * share_fc_out_of_total, "%8.0f") + "\%"
gen share_asset_out_of_FC_str = string(100 * share_asset_out_of_FC, "%8.0f") + "\%"
drop share_asset_out_of_FC share_fc_out_of_total

format AUMmean	AUMmedian	AUM_fcmean	AUM_fcmedian %13.0fc

listtex using "${gcap_data}/output/appendix_figures/table_A_I_fund_sample_summary.tex", replace ///
            rstyle(tabular) ///
            head("\begin{tabularx}{\textwidth}{lccccccc}" ///
                 "\hline  \\" ///
                 "\multirow{2}{*}{} & \multirow{2}{*}{} & \multicolumn{2}{p{2.5cm}}{\centering \textbf{Total AUM (USD mi)}} & \multicolumn{2}{p{2.7cm}}{\centering \textbf{Total FC AUM (USD mi)}} & \multirow{2}{3.5cm}{ \centering \textbf{Average Share of Total AUM in FC Assets}} & \multicolumn{1}{c}{\multirow{2}{4cm}{\centering \textbf{Average Share of FC Assets in LC Government Bonds}}} \tabularnewline" ///
                 "\vspace{0.3cm}" ///
                 " & & Mean & Median & Mean & Median & & \\" ///
                 "\hline  \\" ///
                 "\hline  \\") ///
            foot("\hline" ///
                 "\end{tabularx}")

cap log close