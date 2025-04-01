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
global  emcurr2  `""MXN","MYR","PEN","PHP","PLN","RUB","THB" "'
global  emcurr3 `""KRW","TRY","ZAR","HUF","SGD" "'

global  emcountry1  `""BRA","CHL","COL","CZE","IDN","ISR","IND" "'
global  emcountry2  `""MEX","MYS","PER","PHL","POL","RUS","THA" "'
global  emcountry3 `""KOR","TUR","ZAF","HUN","SGP" "'

global  dmcurr1  `""AUD","CAD","CHF","DKK","EUR","GBP" "'
global  dmcurr2  `""JPY","NOK","NZD","SEK","USD" "'

global  dmcountry1  `""AUS","CAN","CHE","DNK","GBR" "'
global  dmcountry2  `""JPN","NOR","NZL","SWE","USA" "'

global  eu1  `""LUX","IRL","ITA","DEU","FRA","ESP","GRC","NLD","AUT" "'
global  eu2  `""BEL","FIN","PRT","CYP","EST","LAT","LTU","SVK","SVN" "'
global  eu3  `""MLT","EMU","LVA" "'

global eu27 `""HRV","SWE", "POL","ROU","HUN","BGR", "DNK", "CZE" "' 	 	 	 
	 	 
global frontier1 `""ARS","BHD","BDT","XOF","BAM","BWP","BGN","HRK","EGP" "'
global frontier2 `""XAF","GHS","JMD","JOD","KZT","KES","KWD","LBP","MKD" "'
global frontier3 `""MUR","MAD","NAD","NGN","OMR","PKR","PAB","PGK","QUA" "'
global frontier4 `""RSD","LKR","TZS","TTD","TND","UAH","VND","ZMW" "'

global tax_haven_1 `""ABW","AIA","AND","ANT","ATG","BHR","BHS" "'
global tax_haven_2 `""BLZ","BMU","BRB","COK","CRI","CUW","CYM" "'
global tax_haven_3 `""CYP","DJI","DMA","FSM","GGY","GIB","GRD" "'
global tax_haven_4 `""HKG","IMN","IRL","JEY","JOR","KNA","LBN" "'
global tax_haven_5 `""LBR","LCA","LIE","LUX","MAC","MAF","MCO" "'
global tax_haven_6 `""MDV","MHL","MLT","MSR","MUS","NIU","NLD" "'
global tax_haven_7 `""NRU","PAN","SMR","SYC","TCA","TON","VCT" "'
global tax_haven_8 `""VGB","VUT","WSM","SGP" "'

******************************************************************************************************
* CALCULATING HOLDINGS SIMILARITY: UST as reference
******************************************************************************************************

* year to analyze: 
local year "2020"

* defining local for cuts:
local spec=0.5
local aum_cut=0.020
local min_amount=1

* read file
use "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", clear
cap rename date_y year
cap drop cgs_domicile_source
cap drop currency_original

* assets with wrong currency in holdings files
cap drop if currency == "BBB-" 
* adjustment for (very) few Development Bank issuances classified as corporates
cap replace asset_class2 = "Sovereign Bond" if regexm(lower(securityname),"china developm") & currency == "CNY" & asset_class2 == "Corporate Bond"
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

* define countries to analyze
qui mmerge currency using "${gcap_data}/output/morningstar/temp/countries_list.dta", umatch(currency) ukeep(currency) unmatched(m)
keep if _merge==3

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

* classify countries/currencies:
qui gen group_currency = "DM" if currency=="USD"

* keep amount by Master variable year group
qui collapse (sum) marketvalue_usd, by(Master Fund currency year AUM* group_currency Dom share_asset share_fc )
* rounding because we will be dividing by very small numbers: 
replace marketvalue_usd = round(marketvalue_usd,0.0001)
drop AUM_fc
bys MasterPortfolioId: egen AUM_fc = total(marketvalue_usd) // just to be consistent with the rounding
collapse (sum) marketvalue_usd, by(MasterPortfolioId FundName year AUM  currency group_currency AUM_fc AUM_asset Dom share_asset share_fc )
drop if AUM_fc==0

* fillin the currencies for the full panel:  
drop if missing(Master)
fillin Master currency
replace year = `year' if missing(year)
replace marketvalue_usd = 0 if missing(marketvalue_usd)

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
qui gen group_currency = "DM" if currency=="USD"

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

* what is the share if we exclude the currency/country x?
replace dmshare=dmtot/excurr_fundtotal  
quietly {
    foreach x of local curr {    
        replace dmshare=(dmtot-marketvalue_usd)/(excurr_fundtotal) if currency=="`x'" & group_currency=="DM"
    }
}
replace dmshare=. if dmshare<0 // these observations are zeros actually -very irrelevant, just because of rounding
replace dmshare =0 if missing(dmshare)
replace dmshare =0 if abs(dmshare) < 0.001

* to store the correlations: 
gen corr_dm_nospec=.

* dropping the specialists
bys MasterPortfolioId: egen max_share = max(share)
drop if max_share >= `spec'

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_ust.dta", replace

* computing the correlations:
levelsof currency, local(curr)
local not "USD"
local curr_ex: list curr - not
foreach x of local curr_ex {
cap {
    corr share dmshare if currency=="`x'" & share<`spec'
    replace corr_dm_nospec=r(rho) if currency=="`x'"  
    }
}

keep currency corr_dm_nospec year 
duplicates drop
gsort -corr_dm_nospec

* saving correlations 
save "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_ust.dta", replace
cap log close