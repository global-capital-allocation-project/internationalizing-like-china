******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/gravity.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/gravity"

ssc install reghdfe
ssc install outreg2 
ssc install ftools
ssc install matsave
******************************************************************************************************
* PREPARE DATASET FOR ANALYIS: CURRENCY BASED
******************************************************************************************************

* Distance
cap restore
use "${gcap_data}/input/gravity/Gravity_V202202.dta", clear
keep if year == 2020
rename distw_harmonic distwces
drop year
qui mmerge iso3_o using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
gen temp = _merge
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
keep if (_merge == 3 | inlist(iso3_d,$eu1) | inlist(iso3_d,$eu2) | inlist(iso3_d,$eu3)) & (temp == 3 | inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3))
keep iso3_o	iso3_d distwces
replace distwces = 0 if iso3_d == iso3_o
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code)
drop _merge
rename iso_currency_code cur3_d
drop if missing(iso3_o)
collapse (mean) distwces , by(cur3_d iso3_o)
save "${gcap_data}/output/gravity/dist_country_all_curr.dta", replace
gen temp = iso3_o
replace temp = "EMU" if inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3)
collapse (mean) distwces, by(temp cur3_d)
rename temp iso3_o
save "${gcap_data}/output/gravity/dist_country_emu_curr.dta", replace

* Legal System
cap restore
use "${gcap_data}/input/gravity/Gravity_V202202.dta", clear
keep if year == 2020
rename distw_harmonic	distwces
drop year
qui mmerge iso3_o using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
gen temp = _merge
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
keep if (_merge == 3 | inlist(iso3_d,$eu1) | inlist(iso3_d,$eu2) | inlist(iso3_d,$eu3)) & (temp == 3 | inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3))
keep iso3_o	iso3_d legal_new_d legal_new_o
gen legal_equal = 0 
replace legal_equal = 1 if legal_new_o == legal_new_d
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code)
drop _merge
rename iso_currency_code cur3_d
drop if missing(iso3_o)
collapse (median) legal_equal, by(cur3_d iso3_o)
replace legal_equal = 0 if legal_equal == 0.5
save "${gcap_data}/output/gravity/legal_country_all_curr.dta", replace
gen temp = iso3_o
replace temp = "EMU" if inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3)
collapse (median) legal_equal, by(temp cur3_d)
rename temp iso3_o
save "${gcap_data}/output/gravity/legal_country_emu_curr.dta", replace

* Trade
cap restore
use "${gcap_data}/input/gravity/Gravity_V202202.dta", clear
keep if year == 2020
rename distw_harmonic	distwces
drop year
qui mmerge iso3_o using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
gen temp = _merge
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code) unmatched(m)
keep if (_merge == 3 | inlist(iso3_d,$eu1) | inlist(iso3_d,$eu2) | inlist(iso3_d,$eu3)) & (temp == 3 | inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3))
keep iso3_o	iso3_d gdp_o gdp_d tradeflow_imf*
replace tradeflow_imf_d = 0 if iso3_d == iso3_o
replace tradeflow_imf_o = 0 if iso3_d == iso3_o
drop if missing(tradeflow_imf_d) | missing(tradeflow_imf_o)
gen tradeflow_imf = tradeflow_imf_d + tradeflow_imf_o
drop tradeflow_imf_d tradeflow_imf_o
qui mmerge iso3_d using "$gcap_data/input/miscellaneous/country_currency.dta", umatch(iso_country_code)
drop _merge
rename iso_currency_code cur3_d
drop if missing(iso3_o)
collapse (sum) tradeflow_imf gdp_o, by(iso3_o cur3_d)
gen temp = iso3_o
replace temp = "EMU" if inlist(iso3_o,$eu1) | inlist(iso3_o,$eu2) | inlist(iso3_o,$eu3)
collapse (sum) gdp_o tradeflow_imf, by(temp cur3_d)
rename temp iso3_o
gen trade_gdp = tradeflow_imf / gdp_o
gsort -trade_gdp
save "${gcap_data}/output/gravity/trade_country_emu_curr.dta", replace

******************************************************************************************************
* OLS REGRESSIONS
******************************************************************************************************

cap restore
clear

local spec = 0.5
local alt "baseline"
local year = 2020

* read file
use "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_`alt'.dta", clear
unique(Master)
cap drop AUS-ZAF
cap drop AUD-ZAR

* defining specialists
gen any_curr_spec = 0
replace any_curr_spec = 1 if max_share >= `spec' 
gen curr_spec= 0 
replace curr_spec = 1 if share>=`spec'
keep if any_curr_spec == 0

* merging gravity variables (distance, legal, trade)
levelsof currency, local(curr)
unique(Master)
gen temp = DomicileCountryId
replace temp = "EMU" if inlist(Dom,$eu1) | inlist(Dom,$eu2) | inlist(Dom,$eu3)
qui mmerge DomicileCountryId currency using "${gcap_data}/output/gravity/dist_country_all_curr.dta", umatch(iso3_o cur3_d) unmatched(m)
qui mmerge DomicileCountryId currency using "${gcap_data}/output/gravity/legal_country_all_curr.dta", umatch(iso3_o cur3_d) unmatched(m)
qui mmerge temp currency using "${gcap_data}/output/gravity/trade_country_emu_curr.dta", umatch(iso3_o cur3_d) unmatched(m)

replace distwces = ln(distwces)
drop if missing(distwces)
drop if missing(legal_equal)
rename trade_gdp tradeflow_imf_o

label var legal_equal "Legal System"
label var distwces "Distance"
label var tradeflow_imf_o "Trade Flow"

foreach x of local curr {
        qui generate v_`x' = (currency=="`x'")
        qui gen dm_`x' = dmshare * v_`x'
        label var dm_`x' "DMShare_`x'"
        label var v_`x' "`x'"
}
replace tradeflow_imf = ln(tradeflow_imf)
rename currency variable

* reset the file for storing
cap rm "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex"
cap rm "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex"

* Different Regression Specifications

reghdfe share distwces , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , replace tex(frag) label dec(3) addtext(DM Share, No, Fixed Effects, Yes) keep(distwces)
matrix reg1 = e(b)
preserve
matsave reg1, s p("${gcap_data}/temp") replace
restore

reghdfe share tradeflow_imf_o , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, No, Fixed Effects, Yes) keep(tradeflow_imf_o)
matrix reg2 = e(b)
preserve
matsave reg2, s p("${gcap_data}/temp") replace
restore

reghdfe share legal_equal , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, No, Fixed Effects, Yes) keep(legal_equal)
matrix reg3 = e(b)
preserve
matsave reg3, s p("${gcap_data}/temp") replace
restore

reghdfe share distwces tradeflow_imf_o legal_equal , absorb(variable Dom) vce(robust)
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, No, Fixed Effects, Yes) keep(distwces tradeflow_imf_o legal_equal)
matrix reg4 = e(b)
preserve
matsave reg4, s p("${gcap_data}/temp") replace
restore

reghdfe share dm_* distwces , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, Yes, Fixed Effects, Yes) keep(distwces dm_BRL dm_CNY dm_JPY)
matrix reg5 = e(b)
preserve
matsave reg5, s p("${gcap_data}/temp") replace
restore

reghdfe share dm_* tradeflow_imf_o , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, Yes, Fixed Effects, Yes) keep(tradeflow_imf_o dm_BRL dm_CNY dm_JPY)
matrix reg6 = e(b)
preserve
matsave reg6, s p("${gcap_data}/temp") replace
restore

reghdfe share  dm_* legal_equal , absorb(variable Dom) vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, Yes, Fixed Effects, Yes) keep(legal_equal dm_BRL dm_CNY dm_JPY)
matrix reg7 = e(b)
preserve
matsave reg7, s p("${gcap_data}/temp") replace
restore

reghdfe share dm_* distwces tradeflow_imf_o legal_, absorb(variable Dom)  vce(robust) 
qui outreg2 using "${gcap_data}/output/appendix_figures/table_A_III_coefs.tex" , append tex(frag) label dec(3) addtext(DM Share, Yes, Fixed Effects, Yes) keep(distwces tradeflow_imf_o legal_equal dm_BRL dm_CNY dm_JPY)
matrix reg8 = e(b)
preserve
matsave reg8, s p("${gcap_data}/temp") replace
restore

* Saving a file with regression betas by currency

reghdfe share dm_* distwces tradeflow_imf_o legal_, absorb(variable Dom)  vce(robust) 
mat A = r(table)
mat list A
matrix betaOLS= A[1, 1...]'
matsave betaOLS, s p("${gcap_data}/output/gravity") replace
matrix llOLS= A[5, 1...]'
matsave llOLS, s p("${gcap_data}/output/gravity") replace
matrix ulOLS= A[6, 1...]'
matsave ulOLS, s p("${gcap_data}/output/gravity") replace

use "${gcap_data}/output/gravity/betaOLS.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/betaOLS.dta", replace

use "${gcap_data}/output/gravity/llOLS.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/llOLS.dta", replace

use "${gcap_data}/output/gravity/ulOLS.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/ulOLS.dta", replace

******************************************************************************************************
* TOBIT REGRESSIONS
******************************************************************************************************

cap restore
clear

local spec = 0.5
local alt "baseline"
local year = 2020

* read file
use "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_`alt'.dta", clear
unique(Master)
cap drop AUS-ZAF
cap drop AUD-ZAR

* defining specialists
gen any_curr_spec = 0
replace any_curr_spec = 1 if max_share >= `spec' 
gen curr_spec= 0 
replace curr_spec = 1 if share>=`spec'
keep if any_curr_spec == 0

* merging gravity variables (distance, legal, trade)
levelsof currency, local(curr)
unique(Master)
gen temp = DomicileCountryId
replace temp = "EMU" if inlist(Dom,$eu1) | inlist(Dom,$eu2) | inlist(Dom,$eu3)
qui mmerge DomicileCountryId currency using "${gcap_data}/output/gravity/dist_country_all_curr.dta", umatch(iso3_o cur3_d) unmatched(m)
qui mmerge DomicileCountryId currency using "${gcap_data}/output/gravity/legal_country_all_curr.dta", umatch(iso3_o cur3_d) unmatched(m)
qui mmerge temp currency using "${gcap_data}/output/gravity/trade_country_emu_curr.dta", umatch(iso3_o cur3_d) unmatched(m)

replace distwces = ln(distwces)
drop if missing(distwces)
drop if missing(legal_equal)
rename trade_gdp tradeflow_imf_o

label var legal_equal "Legal System"
label var distwces "Distance"
label var tradeflow_imf_o "Trade Flow"

foreach x of local curr {
        qui generate v_`x' = (currency=="`x'")
        qui gen dm_`x' = dmshare * v_`x'
        label var dm_`x' "DMShare_`x'"
        label var v_`x' "`x'"
}
replace tradeflow_imf = ln(tradeflow_imf)
rename currency variable
encode DomicileCountryId, gen(temp2)
encode variable, gen(temp3)

* Saving a file with regression betas by currency

tobit share dm_* distwces tradeflow_imf_o legal_ i.temp2 i.temp3, vce(robust) ll(0) ul(1)
mat A = r(table)
matrix beta= A[1, 1...]'
matsave beta, s p("${gcap_data}/output/gravity") replace
matrix ll= A[5, 1...]'
matsave ll, s p("${gcap_data}/output/gravity") replace
matrix ul= A[6, 1...]'
matsave ul, s p("${gcap_data}/output/gravity") replace

use "${gcap_data}/output/gravity/beta.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"share:dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/beta.dta", replace

use "${gcap_data}/output/gravity/ll.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"share:dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/ll.dta", replace

use "${gcap_data}/output/gravity/ul.dta", clear
keep if _n < 32
replace _rowname = subinstr(_rowname,"share:dm_","",.)
rename _rowname variable
save "${gcap_data}/output/gravity/ul.dta", replace

cap log close


