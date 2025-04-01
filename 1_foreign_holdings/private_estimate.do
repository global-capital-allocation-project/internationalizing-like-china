******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/private_estimate.log", replace

* Install required packages
ssc install kountry

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/foreign_holdings"
cap mkdir "${gcap_data}/output/foreign_holdings/temp"

******************************************************************************************************
* PREPARING BIS Debt Securities
******************************************************************************************************

* Obtaining internationally issued RMB debt outstanding from Chinese issuers in a given year
use "${gcap_data}/input/bis/bis_debt_securities_flat.dta", clear
keep if issuerresidence=="China"
keep if regexm(issuemarket,"International")==1
keep if originalmaturity=="Total (all maturities)"
keep if regexm(ratetype,"All")==1
keep if regexm(remainingmaturity,"Total")==1
keep if measure=="I"
keep if regexm(issuersectori,"All issuers")==1
keep if regexm(issuecurrency,"Total")==1
keep issuecurrencygroup d1* d2*
qui reshape long d, i(issuecurrencygroup) j(quarterstr) str
rename d value
gen quarter=quarterly(quarterstr,"YQ")
format quarter %tq
destring value, replace
gen curr="all" if regexm(issue,"All")==1
replace curr="lc" if regexm(issue,"Domestic")==1
replace curr="fc" if regexm(issue,"Foreign")==1
drop issue quarterstr
reshape wide value, i(quarter) j(curr) str
renpfix value
foreach x in lc fc all {
	replace `x'=`x'/1000
}
gen year=yofd(dofq(quarter))
collapse (lastnm) quarter all fc lc, by(year)
drop q
save "${gcap_data}/output/foreign_holdings/temp/bis_china_currency_y.dta", replace

******************************************************************************************************
* CLEANING AND COMBINING SOURCES FROM CPIS
******************************************************************************************************
 
* first, combine US TIC data: long and short-term debt securities held in China in RMB 
* Tables A6 and A7
use "${gcap_data}/input/miscellaneous/TIC_table_A6_A7_2020.dta", clear
collapse (sum) chn_rmb, by(month)
gen iso_country_code="USA"
save "${gcap_data}/temp/temp_TIC_total.dta", replace

* Import Holdings in Renminbi in CPIS: Table 2
use "${gcap_data}/input/imf_cpis/table_2_updated.dta", clear
drop Indicator period	
rename original_period period
rename *, lower
keep if indicator == "I_A_D_T_CNY_BP6_USD" // this code identifies RMB holdings
kountry ref_area, from(iso2c) to(iso3c) 
rename _ISO iso_country_code
keep period iso_c value
destring value, replace force
drop if iso==""
gen date_str=substr(period,1,4)
replace date_str="June, "+date_str if regexm(period,"S1")==1
replace date_str="December, "+date_str if regexm(period,"S2")==1
gen month=monthly(date_str,"MY")
format month %tm
drop date period
rename value cny
replace cny=. if iso_co=="USA" & month==tm(2020m6) // we are not using mid-year data
* USA data is wrong on CPIS. This value is a conservative estimate using with TIC data. 
qui mmerge month iso_country_code using "${gcap_data}/temp/temp_TIC_total.dta", unmatched(m)
replace cny=chn_rmb if iso_co=="USA" & month==tm(2020m12) 
drop _merge chn_rmb
gen year = year(dofm(month))
gen month_ = month(dofm(month))
drop if month_ == 6
drop month_
duplicates drop
save "${gcap_data}/output/foreign_holdings/temp/table2_rmb.dta", replace

******************************************************************************************************
* COMPUTING MORNINGSTAR SHARES OF RMB HOLDINGS
******************************************************************************************************

* appending HD_for_analysis files
clear
forval y=2014(1)2020 {
  append using "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`y'_y_for_analysis.dta"
}

* restricting to Bonds
keep if asset_class1=="Bond"
rename date_y year

* Saving RMB bonds
preserve
keep if (currency=="CNY" | currency=="CNH")
save "${gcap_data}/output/foreign_holdings/temp/hd_rmb_bonds.dta", replace
restore

* Saving CHN bonds
keep if (cgs_domicile=="CHN" | cgs_domicile=="HKG")
save "${gcap_data}/output/foreign_holdings/temp/hd_chn_bonds.dta", replace

cap drop cgs_domicile_source
cap drop currency_original

* Compute shares RMB in CHN
replace curr="RMB" if curr=="CNY" | curr=="CNH" 
replace curr="Other" if curr~="RMB"
drop if cgs_dom=="HKG"
collapse (sum) marketvalue_usd, by(Dom year curr)
reshape wide mar, i(year Dom) j(curr) str
renpfix marketvalue_usd
rename Other chn_notrmb
rename RMB rmb_chn

* For 2021, assume ratios are the same as 2020 for LUX and IRL / total for TWN (data only for Morningstar) 
set obs `=_N+1'
replace year = 2021 if _n == _N
fillin Dom year
drop if _fillin==1 & year!=2021
drop _fillin
sort Dom year
replace rmb_chn=rmb_chn[_n-1] if year==2021 & inlist(DomicileCountryId,"TWN","LUX","IRL")
replace chn_notrmb=chn_notrmb[_n-1] if year==2021 & inlist(DomicileCountryId,"TWN","LUX","IRL")
drop if missing(rmb_chn) & year==2021
egen total_chn=rowtotal(chn_notrmb rmb_chn)
gen ratio_rmb_of_chn=rmb_chn/total_chn
replace total_chn = 0 if total_chn < .0001
save "${gcap_data}/output/foreign_holdings/temp/temp_ms_shares.dta", replace

* Compute shares CHN in RMB
use "${gcap_data}/output/foreign_holdings/temp/hd_rmb_bonds.dta", clear
cap drop cgs_domicile_source
cap drop currency_original
cap rename date_y year
replace cgs_dom="Other" if cgs_dom~="CHN" & cgs_dom~="HKG" & cgs_dom~="MAC"
collapse (sum) marketvalue_usd, by(DomicileCountry cgs_dom year)
reshape wide marketvalue_usd, i(Dom year) j(cgs_domicile, string)
rename marketvalue_usdCHN temp_rmb_chn
rename marketvalue_usdOther rmb_notchn
* For 2021, assume ratios are the same as 2020 for LUX and IRL / total for TWN (data only for Morningstar) 
set obs `=_N+1'
replace year = 2021 if _n == _N
fillin Dom year
drop if _fillin==1 & year!=2021
drop _fillin
sort Dom year
replace temp_rmb_chn=temp_rmb_chn[_n-1] if year==2021 & inlist(DomicileCountryId,"TWN","LUX","IRL")
replace rmb_notchn=rmb_notchn[_n-1] if year==2021 & inlist(DomicileCountryId,"TWN","LUX","IRL")
drop if missing(temp_rmb_chn) & year==2021
* compute totals and ratios
egen total_rmb=rowtotal(temp_rmb_chn rmb_notchn)
qui mmerge Dom year using "${gcap_data}/output/foreign_holdings/temp/temp_ms_shares.dta"
gen ratio_rmb_chn=total_rmb/total_chn
drop _merge
drop temp_rmb_chn
order Dom year total_chn total_rmb rmb_chn rmb_notchn chn_notrmb ratio_rmb_chn ratio_rmb_of_chn
keep DomicileCountryId	year	total_chn	total_rmb	rmb_chn	rmb_notchn	chn_notrmb	ratio_rmb_chn	ratio_rmb_of_chn
drop if missing(Dom)
save "${gcap_data}/output/foreign_holdings/temp/ms_rmb_share.dta", replace

* Computing aggregate shares:
drop if Dom=="CHN"
collapse (sum) total_chn total_rmb rmb_chn rmb_notchn chn_notrmb, by(year)
sort year
gen ratio_rmb_chn=total_rmb/total_chn
gen ratio_rmb_of_chn=rmb_chn/total_chn
replace ratio_rmb_chn = ratio_rmb_chn[_n-1] if missing(ratio_rmb_chn)
replace ratio_rmb_of_chn = ratio_rmb_of_chn[_n-1] if missing(ratio_rmb_of_chn)
local vars "total_chn	total_rmb	rmb_chn	rmb_notchn	chn_notrmb"
foreach var of varlist `vars'{
    replace `var' = . if `var' ==0
}
replace ratio_rmb_chn = . if year==2021
replace ratio_rmb_of_chn = . if year==2021
save "${gcap_data}/output/foreign_holdings/temp/agg_ms_rmb_share.dta", replace

******************************************************************************************************
* BEGIN ESTIMATING PRIVATE HOLDINGS
******************************************************************************************************

* Considering a threshold:
* we require that we observe in the micro data at least 20\% of the country's 
* bond investment in China (residency) as reported in CPIS
local thresh=.20

* Merge China (CHN) and CNY holdings from CPIS
cap restore
use "${gcap_data}/output/foreign_holdings/temp/imf_cpis_chn_debt.dta", clear
mmerge month iso_co using "${gcap_data}/output/foreign_holdings/temp/table2_rmb.dta"
gen share=cny/chn

* dont use RMB data if RMB holdings greater than China investment:
replace share=. if share>5 
replace cny=. if cny>chn // this could happend because of RMB offshore
gen mnum=month(dofm(month))
cap drop year
gen year=yofd(dofm(month))
keep if mnum==12

* Merge in Morningstar with CPIS
mmerge iso_co year using "${gcap_data}/output/foreign_holdings/temp/ms_rmb_share.dta", umatch(Dom year) uname(ms_) 
gen ystring=year
tostring ystring, replace
gen cyear=iso_co+"_"+ystring
order iso_co year ms_ratio_rmb_chn ms_ratio_rmb_of_chn share
rename share cpis_share

* only keep Morningstar data that passes the threshold
gen chn_ms_cpis_ratio=1000*ms_total_chn/chn
foreach x of varlist ms* {
	replace `x'=. if chn_ms_cpis_ratio<`thresh'
}	
gen ms_share=ms_ratio_rmb_chn
gen share_bg=cpis_share
gen source_share_bg = "CPIS" if cpis_share != .
replace source_share_bg = "MS" if (share_bg==. | iso_co=="CAN") & ms_share != .
replace share_bg=ms_share if share_bg==. | iso_co=="CAN"
save "${gcap_data}/output/foreign_holdings/temp/shares_merged.dta", replace

* Compute average shares
drop if iso_co=="CHN"
replace cny=. if chn==.
replace chn=. if cny==.
drop if iso_co=="WLD"
collapse (mean) cpis_share share_bg (sum) cny chn, by(year)
mmerge year using "${gcap_data}/output/foreign_holdings/temp/agg_ms_rmb_share.dta", uname("agg_ms_") ukeep(ratio_rmb_chn)
drop if _merge==2
rename agg_ms_ratio_rmb_chn ms_share 
foreach x in cpis_share ms_share share_bg {
 	rename `x' mean_`x'
}
gen agg_share=cny/chn
keep year mean* agg

* Use 2016 share estimates for 2014 and 2015
foreach x in mean_cpis_share mean_share_bg mean_ms_share agg_share {
	gen temp=`x' if year==2016
	egen temp2=max(temp)
	replace `x'=temp2 if year==2014 | year==2015
	drop temp temp2
}
save "${gcap_data}/output/foreign_holdings/temp/cny_share.dta", replace
    
* Using best estimate for each country:
use "${gcap_data}/output/foreign_holdings/temp/shares_merged.dta", clear
mmerge year using "${gcap_data}/output/foreign_holdings/temp/cny_share.dta"
drop if iso_co=="CHN"
drop if iso_co=="WLD"
keep if _merge==3

* Making a country specific estimate
gen cny_country_est=chn*share_bg
gen source_cny_country_est = source_share_bg
gen cny_est_cpis=chn*mean_cpis_share
gen cny_est_ms=chn*mean_ms_share
gen cny_est_combine=chn*mean_share_bg
gen cny_est_agg=chn*agg_share

gen source_cny_est_cpis = "mean_cpis_share" if !missing(cny_est_cpis)
gen source_cny_est_ms = "mean_ms_share" if !missing(cny_est_ms)
gen source_cny_est_combine = "mean_share_bg" if !missing(cny_est_combine)
gen source_cny_est_agg = "mean_agg_share" if !missing(cny_est_agg)

* Use RMB for levels if CHN missing in CPIS
gen source_cny = "CPIS" if !missing(cny)
replace source_cny = "MS" if chn==. & ms_total_rmb!=.
replace cny=ms_total_rmb*1000 if chn==.

* Use for cny_country_est
replace cny_country_est=cny if cny~=.
replace source_cny_country_est = source_cny if cny~=.
gen cny_obs=cny
replace cny_obs=1000*ms_total_rmb if cny_obs==.

* Using aggregates for country with no data (except for CAN: data is wrong)
foreach x in cny_est_cpis cny_est_ms cny_est_combine cny_est_agg {
    replace source_`x' = source_cny if cny~=. & iso_co~="CAN"
	replace `x'=cny if cny~=. & iso_co~="CAN"
    replace source_`x' = source_cny_country_est if cny==. & iso_co~="CAN" & cny_country_est~=.
	gen alt_`x'=`x'
	replace alt_`x'=cny_country_est if cny==. & iso_co~="CAN" & cny_country_est~=. 
}
save "${gcap_data}/output/foreign_holdings/temp/interpolation_cpis_pre_emu.dta", replace

* Aggregating EMU countries to the group
replace iso_co="EMU" if inlist(iso_co,$eu1)==1 |  inlist(iso_co,$eu2)==1 |  inlist(iso_co,$eu3)==1 
keep iso_co year cny_est* cny* alt_* source*
rename cny cpis_cny
save "${gcap_data}/output/foreign_holdings/temp/interpolation_cpis.dta", replace

******************************************************************************************************
* ESTIMATING PRIVATE HOLDINGS: Aggregate
******************************************************************************************************

use "${gcap_data}/output/foreign_holdings/temp/interpolation_cpis.dta", clear
collapse (sum) cpis cny* alt_*, by(year)

* merging:
mmerge year using "${gcap_data}/output/foreign_holdings/reserves_estimate.dta" // reserves
mmerge year using "${gcap_data}/output/foreign_holdings/aggregate_holdings_y.dta" // total from Bond Connect
mmerge year using "${gcap_data}/output/foreign_holdings/temp/bis_china_currency_y.dta", ukeep(lc) uname(bis_) // BIS internationally issued RMB debt outstanding

foreach x in cpis_cny cny_est_cpis cny_est_ms cny_est_combine cny_est_agg cny_obs alt_cny_est_cpis alt_cny_est_ms alt_cny_est_combine alt_cny_est_agg cny_country_est {
replace `x'=`x'/1000
}

* computing totals
gen total_foreign_rmb=bc_total_usd+bis_lc
gen cny_estimate=alt_cny_est_combine 
gen cpis_interp=cny_estimate-cny_country_est
gen priv_res=reserves_total+cny_estimate
gen resid=total_foreign_rmb-reserves_total -cny_estimate
gen private_est2=total_foreign_rmb-reserves_total
drop if year<2014 | year>2021

* remove residual from interpolated amounts to exactly match total
* calculate share of private debt in total interpolated amount
gen share_priv_interp=cpis_inter/(cofer_cny_unallocated+cpis_interp)
* scale private and reserves in proportion to their interpolation share
gen balance_private=cpis_interp+share_priv*resid
gen balance_noncofer=cofer_cny_unallocated+(1-share_priv)*resid
    
* confirm bond connect is matching 
gen test=total_foreign_rmb-cny_country_est-balance_private-balance_noncofer-cofer_cny_allocated

* Save file for figure
gen cny_private_balance=balance_private+cny_country_est
gen cny_reserves_balance=balance_noncofer+cofer_cny_allocated
save "${gcap_data}/output/foreign_holdings/temp/decomp_reserves_private.dta", replace

* saving scaling parameter
gen scale_param=cpis_interp/balance_private
keep year share_priv_interp resid scale_param
save "${gcap_data}/output/foreign_holdings/temp/adjustment.dta", replace

******************************************************************************************************
* ESTIMATING PRIVATE HOLDINGS: by country
******************************************************************************************************

cap restore
use "${gcap_data}/output/foreign_holdings/temp/interpolation_cpis_pre_emu.dta", clear
keep iso_co year cny_est* cny* alt_* source*
rename cny cpis_cny
keep if year > 2013 & year < 2022
keep iso year cpis cny* alt_* source*
sort iso year

* merging
mmerge year using "${gcap_data}/output/foreign_holdings/aggregate_holdings_y.dta"  // total from Bond Connect
mmerge year using "${gcap_data}/output/foreign_holdings/temp/bis_china_currency_y.dta", ukeep(lc) uname(bis_) // BIS internationally issued RMB debt outstanding
keep if year > 2013 & year < 2022
drop if iso == "CHN"
drop _merge
foreach x in cpis_cny cny_est_cpis cny_est_ms cny_est_combine cny_est_agg cny_obs alt_cny_est_cpis alt_cny_est_ms alt_cny_est_combine alt_cny_est_agg cny_country_est {
replace `x'=`x'/1000
}

* computing totals
gen total_foreign_rmb=bc_total_usd+bis_lc
gen cny_estimate=alt_cny_est_combine
gen source_cpis_interp = source_cny_est_combine + "_" + source_cny_country_est if !missing(source_cny_est_combine) | !missing(source_cny_country_est)

* using adjustment calculated above (for aggregates)
drop if missing(source_cpis_interp) 
mmerge year using "${gcap_data}/output/foreign_holdings/temp/adjustment.dta"
drop _merge
gen scaled_cny_estimate=cny_estimate
replace scaled_cny_estimate=scaled_cny_estimate/scale_param if cny_country_est==0 | cny_country_est==. 
gen cpis_interp = cny_estimate // either identical, or cny_country_est is missing or CAN
bys year: egen _temp_total = total(cpis_interp)
gen _temp_share = cpis_interp / _temp_total

* remove residual from interpolated amounts to exactly match total
* calculate share of pricate debt in total interpolated amount
* scale private and reserver in proportion to their interpolation share
gen balance_private=cpis_interp+(share_priv*resid*_temp_share) 
replace balance_private = 0 if balance_private <0 // rounding (-1e9)
keep iso_country_code year balance_private cny_country_est source_cpis_interp source_cny_country_est scaled_cny_estimate cny_estimate
gen total_private = scaled_cny_estimate if missing(cny_country_est)
replace total_private = cny_country_est if !missing(cny_country_est)
replace total_private = scaled_cny_estimate if iso_country_code == "CAN"

    * preparing file with top holders for plots 
    preserve
    replace iso_co="EMU" if inlist(iso_co,$eu1)==1 |  inlist(iso_co,$eu2)==1 |  inlist(iso_co,$eu3)==1 
    collapse (sum) total_private scaled_cny_estimate cny_estimate, by(iso_country_code year)
    drop if iso_country_code == "HKG" | iso_country_code == "MAC"
    egen max_cny_row=rowmax(cny_estimate total_private)
    bysort iso_co: egen maxcny=max(max_cny_row)
    replace iso_co="Other" if maxcny<5
    replace iso_co="HK_MAC" if iso_co=="HKG" | iso_co=="MAC"
    collapse (sum) total_private, by(year iso)
    reshape wide total_private, i(year) j(iso_co) str
    unab x: total_private*
    foreach y of varlist `x'* {
        local temp=subinstr("`y'","total_private","",.)
        label var `y' "`temp'"
    }
    save "${gcap_data}/output/foreign_holdings/temp/private_by_selected_countries.dta", replace
    restore

* organizing file to keep reserve and private estimates by country and include the source
keep iso_country_code year total_private source_cpis_interp
replace total_private = total_private*1000 // in billion
replace total_private = round(total_private)
replace source = "Average Share" if regexm(source,"share")
replace source = "Micro Data" if source_cpis_interp == "MS_MS"
rename source source_private
rename total_private estimate_private
gsort -year -estimate_private

* merging reserves data by country
mmerge iso_country_code year using "${gcap_data}/output/foreign_holdings/reserves_estimate_by_country.dta"
drop _merge
gsort -year -estimate_private -estimate_reserve
kountry iso_country_code, from(iso3c)
replace NAMES_STD = "Guernsey" if iso_country_code == "GGY"
replace NAMES_STD = "Jersey" if iso_country_code == "JEY"
replace NAMES_STD = "Other Reserves" if iso_country_code == "Other Reserves"
rename NAMES_STD Investor_Name
rename iso_country_code Investor_Country
rename year Year
rename estimate_private Private_Estimate
rename source_private Private_Source
rename estimate_reserves Reserves_Estimate
rename source_reserves Reserves_Source
order Investor_Country Investor_Name Year Private_Estimate Private_Source Reserves_Estimate Reserves_Source, first
replace Private_Source = "CPIS" if regexm(Private_Source,"CPIS")
drop if Year > 2021
save "${gcap_data}/output/foreign_holdings/foreign_holdings_rmb_bonds.dta", replace

cap log close