******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/foreign_holdings_figures.log", replace

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/paper_figures"
cap mkdir "${gcap_data}/output/appendix_figures"


******************************************************************************************************
* PLOTTING FIGURES FOR THE PAPER/APPENDIX
******************************************************************************************************

* Paper Figure 1: Preparing the file
use "${gcap_data}/output/foreign_holdings/foreign_holdings_rmb_bonds.dta", clear
drop if Year > 2021
keep Investor_Country Private_Estimate Reserves_Estimate Year
rename Private_Estimate est_Private
rename Reserves_Estimate est_Reserves
reshape long est_, i(Year Investor_Country) j(type, string)
rename est_ Estimate
replace Investor_Country = "COFER Observable" if type == "Reserves" & !inlist(Investor_Country,"RUS","Other Reserves")
replace Investor_Country="EMU" if (inlist(Investor_Country,$eu1)==1 |  inlist(Investor_Country,$eu2)==1 |  inlist(Investor_Country,$eu3)==1) & type == "Private"
replace Investor_Country="Other" if !inlist(Investor_Country,"HKG","EMU","USA","SGP","GBR") & type == "Private"
collapse (sum) Estimate, by(Year Investor_Country type)
replace Estimate = Estimate /1e3
reshape wide Estimate, i(Year Investor_Country) j(type, string)
replace Investor_Country = subinstr(Investor_Country," ","",.)
reshape wide EstimatePrivate	EstimateReserves, i(Year) j(Investor_Country, string)

keep Year EstimateReservesCOFERObservable EstimatePrivateEMU EstimatePrivateHKG  EstimatePrivateOther EstimateReservesOtherReserves EstimateReservesRUS	EstimatePrivateGBR EstimatePrivateUSA EstimatePrivateSGP
renpfix Estimate

gen Reserves = ReservesRUS+ReservesCOFERObservable+ReservesOtherReserves if Year < 2021
*For plotting 2021 by country, set Reserves==0 and Private==0
replace Reserves = 0 if Year == 2021
gen Private = PrivateHKG+PrivateEMU+PrivateUSA+PrivateSGP+PrivateGBR+PrivateOther if Year < 2021
replace Private = 0 if Year == 2021
foreach x in ReservesRUS ReservesCOFERObservable ReservesOtherReserves PrivateHKG PrivateEMU PrivateUSA PrivateSGP PrivateGBR PrivateOther {
    replace `x'=0 if Year < 2021
}
rename Reserves ReservesTotal 
rename Private PrivateTotal
qui reshape long Reserves Private, i(Year) j(temp, string)
replace Private = . if Private==0
replace Reserves = . if Reserves==0
drop if missing(Private) & missing(Reserves)
rename Private TotalPrivate
rename Reserves TotalReserves
reshape long Total, i(Year temp) j(value, string)
drop if missing(Total)
rename Total Amount
rename value InvestorType 
rename temp Investor
save "$gcap_data/output/foreign_holdings/data_figure_cover.dta", replace

* Plotting figure 1 
use "$gcap_data/output/foreign_holdings/data_figure_cover.dta", clear
qui reshape wide Amount, i(InvestorType Year) j(Investor, string)
replace InvestorType = "_" + InvestorType
qui reshape wide Amount*, i(Year) j(InvestorType, string)
gen year = Year
gen month=12
sort year

*variables for plot: 
replace AmountTotal_Private=AmountTotal_Private+AmountTotal_Reserves if year !=2021
replace AmountCOFERObservable_Reserves=AmountRUS_Reserves+ AmountCOFERObservable_Reserves if year ==2021
replace AmountOtherReserves_Reserves = AmountOtherReserves_Reserves + AmountCOFERObservable_Reserves if year ==2021
replace AmountHKG_Private = AmountOtherReserves_Reserves + AmountHKG_Private if year==2021
replace AmountEMU_Private = AmountHKG_Private + AmountEMU_Private if year==2021
replace AmountUSA_Private = AmountEMU_Private + AmountUSA_Private if year==2021
replace AmountSGP_Private = AmountSGP_Private + AmountUSA_Private if year==2021
replace AmountGBR_Private = AmountSGP_Private + AmountGBR_Private if year==2021
replace AmountOther_Private = AmountGBR_Private + AmountOther_Private if year==2021

drop if year > 2021
cap drop x
gen x=year
local bwitdh = 0.7

twoway (bar AmountTotal_Reserves x if month == 12 & year != 2021, barwidth(0.7) fcolor(blue) lcolor(blue)) ///  
       (rbar AmountTotal_Reserves AmountTotal_Private x  if month == 12 & year != 2021, barwidth(0.7) fcolor(none) lcolor(red) lwidth(thick)) ///
       (bar AmountRUS_Reserves x if month == 12 & year == 2021, barwidth(0.7) lwidth(medthick) fcolor(blue) lcolor(blue) fintensity(15)) ///
       (rbar AmountRUS_Reserves AmountCOFERObservable_Reserves  x if month == 12 & year == 2021, barwidth(0.7) lwidth(medthick) fcolor(blue) lcolor(blue) fintensity(40)) ///
       (rbar AmountCOFERObservable_Reserves AmountOtherReserves_Reserves x  if month == 12 & year == 2021, barwidth(0.7) lwidth(medthick) lpattern(solid) fcolor(blue) lcolor(blue) fintensity(60)) ///
       (rbar AmountOtherReserves_Reserves AmountHKG_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(15)) ///
       (rbar AmountHKG_Private AmountEMU_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(30)) ///
       (rbar AmountEMU_Private AmountUSA_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(45)) ///
       (rbar AmountUSA_Private AmountSGP_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(60)) ///
       (rbar AmountSGP_Private AmountGBR_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(75)) ///
       (rbar AmountGBR_Private AmountOther_Private x  if month == 12 & year == 2021, barwidth(`bwitdh') lwidth(medthick) lpattern(solid) fcolor(red) lcolor(red) fintensity(25)), ///
       ytitle("USD Billions" " ") ///
       ylabel(0(100)600)  yscale(r(0(100)600)) xscale(r(2014(1)2021)) xlabel(2014(1)2021, nogrid) ///
       xlabel( 2014 "2014" 2015 "2015" 2016 "2016" 2017 "2017" 2018 "2018" 2019 "2019" 2020 "2020" 2021 "2021" , tlength(0)) ///
       xtitle("", axis(1)) ///
       graphregion(color("white")) ///
       legend(order(1 "Reserves" 2 "Private" "Investments") cols(3) region(lcolor(black)))
graph export "$gcap_data/output/paper_figures/1_breakdown_updated_2021.eps", as(eps) replace
graph export "$gcap_data/output/paper_figures/1_breakdown_updated_2021.pdf", as(pdf) replace

* Appendix Figure A.I:  Geography of Private Holders of Renminbi Bonds
use "${gcap_data}/output/foreign_holdings/temp/private_by_selected_countries.dta", clear
drop if year > 2021
graph bar (asis) total_privateEMU total_privateUSA total_privateSGP total_privateJPN total_privateTWN total_privateGBR total_privateOther , over(year) stack legend(size(small) rows(2)) graphregion(color(white)) name("scaled_cny_estimate_exhk",replace) ///
    bar(1, fintensity(60)) bar(2, fintensity(60)) bar(3, fintensity(40)) bar(4, fintensity(40)) bar(5, fintensity(40)) bar(6, fintensity(40)) bar(7, fintensity(60) lcolor(gray) fcolor(gray)) ytitle("USD Billions" " ") 
graph export "${gcap_data}/output/appendix_figures/A_I_geography_private_holders.eps", replace as(eps)

* Appendix Figure A.II: The World’s Largest Bond Markets
use "${gcap_data}/input/bis/bis_debt_securities_flat.dta", clear
keep if regexm(series,"3P:1:1:1:A:A:TO1:A:A:A:A:A:I")
kountry issuer_res, from(iso2c) to(iso3c)
drop if missing(_ISO3C_)
rename _ISO3C_ res
gen eu = 0
replace eu=1 if inlist(res, $eu1) | inlist(res, $eu2) | inlist(res, $eu3) | inlist(res,$eu27)
replace res = "EU27" if eu == 1
collapse (sum) d1* d2* , by(res)
qui reshape long d, i(res) j(temp, string)
rename d v
gen quarter = real(substr(temp,6,1))
gen year = real(substr(temp,1,4))
keep if quarter == 4
keep if year > 2005
keep if year < 2022
drop temp quarter
keep if inlist(res,"USA","CHN","EU27","GBR","JPN")
replace v = v/1e6
reshape wide v, i(year) j(res, string)
tw connected vJPN vUSA vEU27 vGBR  vCHN   year , graphregion(color(white)) legend(order(1 "JPN" 2 "USA" 3 "EU27" 4 "GBR" 5 "CHN") col(5) size(small)) lcolor(blue black orange green red) msize(small small small small medium) mlcolor(blue black orange green red) mfcolor(white white white white red) xscale(range(2006 2022)) ytitle("Dollar trn" " ") xtitle("")
graph export "${gcap_data}/output/appendix_figures/A_II_largest_bond_markets.eps", replace as(eps)

* Figure A.III: Quarterly Foreign Ownership of RMB-Denominated bonds
use "${gcap_data}/output/foreign_holdings/aggregate_holdings_q.dta", clear
gen month=month(dofm(date_m))
keep if inlist(month,3,6,9,12)
gen date_q = yq(year(dofm(date_m)),quarter(dofm(date_m)))
format %tq date_q
tw connect bc_total_rmb date_m, msize(small) mfcolor(white) mlcolor(black) graphregion(color(white)) ytitle("RMB Billions" " ") xtitle("") xla(660(12)768, format(%tmCCYY)) xscale(r(660(12)776)) ylabel(, angle(horizontal)) lcolor(black)
graph export  "$gcap_data/output/appendix_figures/A_III_total_foreign_quarterly_rmb.eps", as(eps) replace
tw connect bc_total_usd date_m , msize(small) mfcolor(white) mlcolor(black) graphregion(color(white)) ytitle("USD Billions" " ") xtitle("") xla(660(12)768, format(%tmCCYY)) xscale(r(660(12)776))  yscale(r(0(100)700)) ylabel(0(100)700, angle(horizontal)) lcolor(black)
graph export  "$gcap_data/output/appendix_figures/A_III_total_foreign_quarterly_usd.eps", as(eps) replace

* Paper Figure 8: Foreign Ownership of RMB-Denominated Bonds in the 2015-16 Episode
tw (connect bc_total_rmb date_q if inlist(month,3,6,9,12) & date_m >= tm(2015m1) & date_m <= tm(2016m9)) /// 
    (connect bc_total_usd date_q if inlist(month,3,6,9,12) & date_m >= tm(2015m1) & date_m <= tm(2016m9), yaxis(2)), ///
    xtitle("") ytitle("RMB Billions" " ") ytitle(" " "USD Billions", axis(2)) legend(order(1 "RMB (Left Axis)" 2 "USD (Right Axis)")) ylabel(, angle(horizontal)) ylabel(, axis(2) angle(horizontal)) graphregion(color(white))
graph export "$gcap_data/output/paper_figures/8_total_foreign_quarterly_zoom.eps", replace

* Appendix Figure IV: The Composition of Foreign Ownership of RMB Bonds
use "${gcap_data}/output/foreign_holdings/holdings_decomposition.dta", clear
graph bar (asis) share_fo, over(bondtype, sort(foreign) descending label(angle(55) labsize(small))) graphregion(color(white))
graph export "${gcap_data}/output/appendix_figures/A_IV_share_foreign_investors_2021m12.eps", replace
egen foreign_total=sum(foreign_investors) if bondtype~="Aggregate"
egen aggregate=sum(total) if bondtype~="Aggregate"
gen share_of_foreign=foreign_in/foreign_total
gen share_of_total=total/aggregate
drop aggregate
graph bar (asis) share_of_fo share_of_to if bondtype~="Aggregate", over(bondtype, sort(share_of_fo) descending label(angle(55) labsize(small))) graphregion(color(white)) legend(order(1 "Foreign" 2 "Total"))
graph export "${gcap_data}/output/appendix_figures/A_IV_total_foreign_2021m12.eps", replace

* Appendix Figure V: 
use "$gcap_data/output/foreign_holdings/temp/rmb_domestic_aggregated.dta", clear
twoway (line intl_share year, lwidth(thick))  if year>=2012, xtitle("") graphregion(color(white)) ytitle("Share") 
graph export "${gcap_data}/output/appendix_figures/A_V_offshore_share_ts.eps", replace
use  "${gcap_data}/output/foreign_holdings/temp/collapse.dta", clear
replace curr="RMB" if curr=="CNY" | curr=="CNH"
replace curr="FC" if curr~="RMB"
keep if curr=="FC"
replace country_type="Nationality" if country_type~="CHN"
collapse (sum) marketvalue_usd, by(curr country_type year)
replace country_type=subinstr(country_type," ","_",.)
gen bond_type=country_type+"_"+currency
drop country_type curr
append using "${gcap_data}/output/foreign_holdings/temp/rmb_domestic_aggregated_long.dta"
reshape wide marketvalue_usd, i(year) j(bond_type) str
foreach x of varlist marketvalue_usd* {
    replace `x'=0 if `x'==.
}
renpfix marketvalue_usd
keep if year > 2013
graph bar (asis) domestic_rmb intl_rmb CHN_FC Nationality_FC if year>=2012, over(year) stack graphregion(color(white)) legend(order(1 "Onshore RMB" 2 "Offshore RMB" 3 "China Residency FC" 4 "China Nationality FC") rows(2)) bar(1, fcolor(cranberry) fintensity(40) lcolor(maroon)) ///
bar(2, fcolor(cranberry) lcolor(cranberry%100)) bar(3, fcolor(navy) lcolor(navy) fintensity(40)) bar(4, fcolor(ltblue%75) fintensity(75) lcolor(navy))
graph export "${gcap_data}/output/appendix_figures/A_V_world_timeseries_aggcurr_alt.eps", replace

cap log close