******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
global gcap_data="`1'"

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/holdings_similarity.log", replace

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/holdings_similarity"
cap mkdir "${gcap_data}/output/holdings_similarity/temp"
cap mkdir "${gcap_data}/output/morningstar/temp"

* Global variables:
global  emcurr1  `""BRL","CLP","COP","CZK","IDR","ILS","INR" "'
global  emcurr2  `""MXN","MYR","PEN","PHP","PLN","RON","RUB","THB" "'
global  emcurr3 `""KRW","TRY","ZAR" "'

global  dmcurr1  `""AUD","CAD","CHF","DKK","EUR","GBP" "'
global  dmcurr2  `""JPY","NOK","NZD","SEK","USD" "'

******************************************************************************************************
* CALCULATING HOLDINGS SIMILARITY: Baseline LC_Govt Bonds by Currency
******************************************************************************************************

* year to analyze: 
local year "2020"

* defining local for cuts:
local spec=0.5
local aum_cut=0.020
local min_amount=1

use "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_baseline.dta", clear

* computing the correlations:
levelsof currency, local(curr)
foreach x of local curr {
cap {
    corr share dmshare [aweight = AUM_fc] if currency=="`x'" & share<`spec'
    replace corr_dm_nospec=r(rho) if currency=="`x'"  
    }
}

keep currency corr_dm_nospec year 
duplicates drop
gsort -corr_dm_nospec

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_weighted.dta", replace

cap log close