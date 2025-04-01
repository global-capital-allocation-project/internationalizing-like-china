******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/holdings_similarity_figs.log", replace

* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/paper_figures"
cap mkdir "${gcap_data}/output/appendix_figures"

ssc install labutil
ssc install listtex
ssc install mmerge
ssc install fabplot
******************************************************************************************************
* HOLDINGS SIMILARITY FIGURE: HOLDINGS SCATTER PLOTS
******************************************************************************************************

* Paper Figure 3

* year to analyze:
local year=2020

* PIMCO Emerging Markets Local Currency and Bond Fund:
local fund1=2633
* T. Rowe Price International Bond Fund:
local fund2=187901

* load file
use "${gcap_data}/output/holdings_similarity/holdings_LC_Govt_currency_`year'_baseline.dta", clear
cap rename currency variable
cap drop temp

* adjustment for fitted line for BRL (holdings can't be < 0)
qui reg share dmshare if variable == "BRL"
predict temp if variable == "BRL"
sum temp, meanonly
local max = r(max)
local min = r(min)

tw (scatter share dmshare if variable == "BRL", msize(small) msymbol(Oh) mcolor(gray)) (line temp dmshare if variable == "BRL" & temp >=0) ///
    (scatter share dmshare if MasterPortfolioId == `fund1' &  variable == "BRL" & inlist(variable,"CNY","JPY","BRL"), msize(large) mcolor(red)) ///
    (scatter share dmshare if MasterPortfolioId == `fund2' &  variable == "BRL"  & inlist(variable,"CNY","JPY","BRL"), msize(large) mcolor(blue)), ///
    ytitle("") xtitle("") yscale(r(0 0.4)) ylabel(0 (0.1) 0.4) graphregion(color(white)) name(gBRL, replace) ///
    legend(off) ytitle("Share of Foreign Currency Portfolio in BRL" " ", size(medium)) xtitle(" " "Share of Foreign Currency Portfolio in DM Currencies", size(medium))
graph export "${gcap_data}/output/paper_figures/3_scatter_BRL.eps", as(eps) replace

local scatters "CNY JPY"
foreach x in `scatters' {
tw (scatter share dmshare if variable == "`x'", msize(small) msymbol(Oh) mcolor(gray)) (lfit share dmshare if variable == "`x'") ///
    (scatter share dmshare if MasterPortfolioId == `fund1' &  variable == "`x'" & inlist(variable,"CNY","JPY","BRL"), msize(large) mcolor(red)) ///
    (scatter share dmshare if MasterPortfolioId == `fund2' &  variable == "`x'"  & inlist(variable,"CNY","JPY","BRL"), msize(large) mcolor(blue)), ///
    ytitle("") xtitle("") yscale(r(0 0.4)) ylabel(0 (0.1) 0.4) graphregion(color(white)) name(g`x', replace) ///
    legend(off) ytitle("Share of Foreign Currency Portfolio in `x'" " ", size(medium)) xtitle(" " "Share of Foreign Currency Portfolio in DM Currencies", size(medium))
graph export "${gcap_data}/output/paper_figures/3_scatter_`x'.eps", as(eps) replace
}

* Appendix Figure A.VII

local graphs ""
levelsof variable, local(curr)
cap drop temp
foreach x in `curr' {
    qui reg share dmshare if variable == "`x'"
    predict temp if variable == "`x'"
    sum temp, meanonly
    local max = r(max)
    local min = r(min)
    if (`max' > 1 | `min' < 0) {
    qui tw (scatter share dmshare if variable == "`x'", msize(small) msymbol(Oh) mcolor(gray)) (lfit temp dmshare if variable == "`x'" & temp >=0 & temp <=1, lwidth(medthick) lcolor(black)) ///
    (scatter share dmshare if MasterPortfolioId == `fund1' &  variable == "`x'" , msize(large) mcolor(red)) ///
    (scatter share dmshare if MasterPortfolioId == `fund2' &  variable == "`x'"  , msize(large) mcolor(blue)), ///
    ytitle("") xtitle("") yscale(r(0 0.4)) ylabel(0 (0.1) 0.4) graphregion(color(white)) name(g`x', replace) ///
    legend(off) ytitle("") xtitle("", size(medium)) title("`x'", color(black) size(medsmall))
    local graphs "`graphs' g`x'"
    }
    if (`max' < 1 & `min' > 0) {
    qui tw (scatter share dmshare if variable == "`x'", msize(small) msymbol(Oh) mcolor(gray)) (lfit share dmshare if variable == "`x'", lwidth(medthick) lcolor(black)) ///
    (scatter share dmshare if MasterPortfolioId == `fund1' &  variable == "`x'" , msize(large) mcolor(red)) ///
    (scatter share dmshare if MasterPortfolioId == `fund2' &  variable == "`x'"  , msize(large) mcolor(blue)), ///
    ytitle("") xtitle("") yscale(r(0 0.4)) ylabel(0 (0.1) 0.4) graphregion(color(white)) name(g`x', replace) ///
    legend(off) ytitle("") xtitle("", size(medium)) title("`x'", color(black) size(medsmall))
    local graphs "`graphs' g`x'"
    }
    drop temp*
}

graph combine `graphs', col(5) graphregion(color(white))
graph export "${gcap_data}/output/appendix_figures/A_VII_scatter_all_currencies_2funds.eps", as(eps) replace

******************************************************************************************************
* HOLDINGS SIMILARITY FIGURE: CORRELATIONS
******************************************************************************************************

* Paper Figure 4:

local year "2020"

* load file
use "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_baseline.dta", clear

* define currency groupc
gen group_currency = "EM" if(inlist(currency,$emcurr1) | inlist(currency,$emcurr2) | inlist(currency,$emcurr3))
replace group_currency = "DM" if(inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2) )
replace group_currency = "CNY" if currency == "CNY"
assert !missing(group_currency)

* organize data for plotting
drop if missing(corr_dm_nospec)
duplicates drop
gsort -corr_dm_nospec
encode currency, gen(_curr)
gen nval = _n
reshape wide corr_ interval*, i(nval currency) j(group_currency, string)
labmask nval, val(currency)
lab var nval "currency"
sum nval, meanonly
local max = r(max)
    
* bar plots with confidence intervals. dm_spec refers to Developed Markets excluding Specialist funds as our baseline. 
twoway (bar corr_dm_nospecEM nval, barw(0.85) xlabel(1(1)`max', labsize(vsmall) labels angle(45) valuelabel) fintensity(60) fcol(olive_teal) lcolor(green)) (rcap interval_upperEM interval_lowerEM nval, lcolor(gray)) || ///
       (bar corr_dm_nospecCNY nval, barw(0.85) fintensity(40) fcol(cranberry) lcolor(maroon)) (rcap interval_upperCNY interval_lowerCNY nval, lcolor(gray)) || ///
       (bar corr_dm_nospecDM nval , barw(0.85) fintensity(60) fcol(ltblue) lcolor(navy)) (rcap interval_upperDM interval_lowerDM nval, lcolor(gray)), yla(-0.5(0.5)0.5) graphregion(color(white)) /// 
       plotregion(lcolor(black)) yline(0, lcolor(black)) ytitle("Correlation with DM Positions" " ") xtitle("") legend(order(1  "EM" 3 "CNY" 5 "DM" ) cols(4))
graph export "${gcap_data}/output/paper_figures/4_holdings_similarity_CI.eps", replace as(eps)

* Appendix Figures A.VIII: alternative specifications

local run_type "ust weighted excl_index intensive_mg alt_thr alt_aum alt_fc"
local year "2020"

clear
foreach alt in `run_type' {
    * load file
    use "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_`alt'.dta", clear

    * define currency groupc
    gen group_currency = "EM" if(inlist(currency,$emcurr1) | inlist(currency,$emcurr2) | inlist(currency,$emcurr3))
    replace group_currency = "DM" if(inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2) )
    replace group_currency = "CNY" if currency == "CNY"
    assert !missing(group_currency)

    * organize data for plotting
    cap drop interval*
    drop if missing(corr_dm_nospec)
    duplicates drop
    gsort -corr_dm_nospec
    encode currency, gen(_curr)
    gen nval = _n
    reshape wide corr_ , i(nval currency) j(group_currency, string)
    labmask nval, val(currency)
    lab var nval "currency"
    sum nval, meanonly
    local max = r(max)


    * bar plots 
    twoway (bar corr_dm_nospecEM nval, barw(0.85) xlabel(1(1)`max', labsize(vsmall) labels angle(45) valuelabel) fintensity(60) fcol(olive_teal) lcolor(green) lpattern(solid)) || (bar corr_dm_nospecCNY nval, barw(0.85) fintensity(40) fcol(cranberry) lcolor(maroon) lpattern(solid)) ///
    || (bar corr_dm_nospecDM nval, barw(0.85) fintensity(60) fcol(ltblue) lcolor(navy) lpattern(solid)), yla(-0.5(0.5)0.5) graphregion(color(white)) plotregion(lcolor(black)) yline(0, lcolor(black) lpattern(solid)) ytitle("Correlation with DM Positions" " ") xtitle("") legend(order(1  "EM" 2 "CNY" 3 "DM" ) cols(4)) 
    graph export "${gcap_data}/output/appendix_figures/A_VIII_holdings_similarity_`alt'.eps", replace as(eps)
}

* Appendix Figure A.X: time series plots

clear
foreach year of numlist 2014/2020 {
    append using "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_baseline.dta"
}

rename currency variable

gen group_variable = "DM" if (inlist(variable,$dmcurr1) | inlist(variable,$dmcurr2)) 
replace group_variable = "EM" if missing(group_variable) & (inlist(variable,$emcurr1) | inlist(variable,$emcurr2) | inlist(variable,$emcurr3))
replace group_variable = "CNY" if variable == "CNY"

fabplot line corr_ year, by(variable, note("") compact) xtitle("") xla(2014/2020, format(%tyY)) c(L) ///
yla(`yla', ang(h)) ytitle("Correlation with DM Positions", size(small)) subtitle(, fcolor(ltblue*0.4)) ///
front(connect) frontopts(mc(blue) lc(blue) lw(thick)) yline(0, lcolor(black)) scheme(s1color) graphregion(color(white)) 
graph export "${gcap_data}/output/appendix_figures/A_X_time_series_all_countries.eps", replace as(eps)


* Appendix Figure A.XI: other assets

local run_type "usd_bonds equity"
local year "2020"

clear
foreach alt in `run_type' {
    * load file
    use "${gcap_data}/output/holdings_similarity/correlations_`alt'_nationality_`year'.dta", clear

    * define country group
    gen group_currency = "EM" if(inlist(country_bg,$emcountry1) | inlist(country_bg,$emcountry2) | inlist(country_bg,$emcountry3))
    replace group_currency = "DM" if(inlist(country_bg,$dmcountry1) | inlist(country_bg,$dmcountry2) | country_bg=="EMU")
    replace group_currency = "CHN" if country_bg== "CHN"
    drop if missing(group_currency)

    * organize data for plotting
    drop if missing(corr_dm_nospec)
    duplicates drop
    gsort -corr_dm_nospec
    encode country_bg, gen(_curr)
    gen nval = _n
    reshape wide corr_ interval*, i(nval country_bg) j(group_currency, string)
    labmask nval, val(country_bg)
    lab var nval "country_bg"
    sum nval, meanonly
    local max = r(max)

    
    
    * bar plots with confidence intervals
    twoway (bar corr_dm_nospecEM nval, barw(0.85) xlabel(1(1)`max', labsize(vsmall) labels angle(45) valuelabel) fintensity(60) fcol(olive_teal) lcolor(green) lpattern(solid)) (rcap interval_upperEM interval_lowerEM nval, lcolor(gray))  ///
    || (bar corr_dm_nospecCHN nval, barw(0.85) fintensity(40) fcol(cranberry) lcolor(maroon) lpattern(solid)) (rcap interval_upperCHN interval_lowerCHN nval, lcolor(gray)) ///
    || (bar corr_dm_nospecDM nval, barw(0.85) fintensity(60) fcol(ltblue) lcolor(navy) lpattern(solid)) (rcap interval_upperDM interval_lowerDM nval, lcolor(gray)), yla(-0.5(0.5)0.5) graphregion(color(white)) plotregion(lcolor(black)) ///
    yline(0, lcolor(black) lpattern(solid)) ytitle("Correlation with DM Positions" " ") xtitle("") legend(order(1  "EM" 2 "CHN" 3 "DM" ) cols(4)) 
    graph export "${gcap_data}/output/appendix_figures/A_XI_holdings_similarity_holdings_similarity_`alt'.eps", replace as(eps)
}

* Appendix Figure A.IX: Cross-Section of Beta Estimates in 2020

* Panel (a): OLS
use "${gcap_data}/output/gravity/betaOLS.dta", clear
qui mmerge variable using "${gcap_data}/output/gravity/ulOLS.dta"
qui mmerge variable using  "${gcap_data}/output/gravity/llOLS.dta"
drop _merge

gen group_variable = "Frontier" if inlist(variable,$frontier1) | inlist(variable,$frontier2) 
replace group_variable = "Frontier" if inlist(variable,$frontier3) | inlist(variable,$frontier4) 
replace group_variable = "DM" if (inlist(variable,$dmcurr1) | inlist(variable,$dmcurr2)) & missing(group_variable)
replace group_variable = "DM" if (inlist(variable,$dmcountry1) | inlist(variable,$dmcountry2) | variable == "EMU") & missing(group_variable)
replace group_variable = "EM" if missing(group_variable) & (inlist(variable,$emcurr1) | inlist(variable,$emcurr2) | inlist(variable,$emcurr3))
replace group_variable = "EM" if missing(group_variable) & (inlist(variable,$emcountry1) | inlist(variable,$emcountry2) | inlist(variable,$emcountry3))
replace group_variable = "CNY" if variable == "CNY"
replace group_variable = "CNY" if variable == "CHN"
drop if missing(group_variable)

drop if missing(b)
duplicates drop
gsort -b
encode variable, gen(_curr)
gen nval = _n

reshape wide b ll ul, i(nval variable) j(group_variable, string)
labmask nval, val(variable)
lab var nval "variable"
sum nval, meanonly
local max = r(max)

twoway (bar bEM nval, barw(0.85) xlabel(1(1)`max', labsize(vsmall) labels angle(45) valuelabel) fintensity(60) fcol(olive_teal) lcolor(green)) (rcap ulEM llEM nval, lcolor(gray)) || (bar bCNY nval, barw(0.85) fintensity(40) fcol(cranberry) lcolor(maroon)) (rcap ulCNY llCNY nval, lcolor(gray)) ///
    || (bar bDM nval , barw(0.85) fintensity(60) fcol(ltblue) lcolor(navy)) (rcap ulDM llDM nval, lcolor(gray)),  graphregion(color(white)) plotregion(lcolor(black)) yline(0, lcolor(black)) ytitle("Estimated Beta - OLS" " ") xtitle("") legend(order(1  "EM" 3 "CNY" 5 "DM" ) cols(4))
graph export "${gcap_data}/output/appendix_figures/A_IX_fig_beta_ols_CI.eps", replace as(eps)

* Panel (b): TOBIT

use "${gcap_data}/output/gravity/beta.dta", clear
qui mmerge variable using "${gcap_data}/output/gravity/ul.dta"
qui mmerge variable using "${gcap_data}/output/gravity/ll.dta"
drop _merge

gen group_variable = "Frontier" if inlist(variable,$frontier1) | inlist(variable,$frontier2) 
replace group_variable = "Frontier" if inlist(variable,$frontier3) | inlist(variable,$frontier4) 
replace group_variable = "DM" if (inlist(variable,$dmcurr1) | inlist(variable,$dmcurr2)) & missing(group_variable)
replace group_variable = "DM" if (inlist(variable,$dmcountry1) | inlist(variable,$dmcountry2) | variable == "EMU") & missing(group_variable)
replace group_variable = "EM" if missing(group_variable) & (inlist(variable,$emcurr1) | inlist(variable,$emcurr2) | inlist(variable,$emcurr3))
replace group_variable = "EM" if missing(group_variable) & (inlist(variable,$emcountry1) | inlist(variable,$emcountry2) | inlist(variable,$emcountry3))
replace group_variable = "CNY" if variable == "CNY"
replace group_variable = "CNY" if variable == "CHN"
drop if missing(group_variable)

drop if missing(b)
duplicates drop
gsort -b
encode variable, gen(_curr)
gen nval = _n
reshape wide b ll ul, i(nval variable) j(group_variable, string)
labmask nval, val(variable)
lab var nval "variable"
sum nval, meanonly
local max = r(max)

twoway (bar bEM nval, barw(0.85) xlabel(1(1)`max', labsize(vsmall) labels angle(45) valuelabel) fintensity(60) fcol(olive_teal) lcolor(green)) (rcap ulEM llEM nval, lcolor(gray)) || (bar bCNY nval, barw(0.85) fintensity(40) fcol(cranberry) lcolor(maroon)) (rcap ulCNY llCNY nval, lcolor(gray)) ///
    || (bar bDM nval , barw(0.85) fintensity(60) fcol(ltblue) lcolor(navy)) (rcap ulDM llDM nval, lcolor(gray)),  graphregion(color(white)) plotregion(lcolor(black)) yline(0, lcolor(black)) ytitle("Estimated Beta - Tobit" " ") xtitle("") legend(order(1  "EM" 3 "CNY" 5 "DM" ) cols(4))
graph export "${gcap_data}/output/appendix_figures/A_IX_fig_beta_tobit_CI.eps", replace as(eps)

******************************************************************************************************
* OTHER APPENDIX FIGURES
******************************************************************************************************

* Appendix Figure A.XII: Returns on RMB relative to EM and DM Currencies

use "${gcap_data}/output/bond_betas/hml_vix_full.dta", clear
local time "quarter"
local sample "post2010"

foreach x in hml vix {
gen `x'_2se_plus=b_`x'+2*se_`x'
gen `x'_2se_minus=b_`x'-2*se_`x'

gen alpha_`x'_2se_plus=alpha_`x'+2*se_alpha_`x'
gen alpha_`x'_2se_minus=alpha_`x'-2*se_alpha_`x'

gen fx_`x'_2se_plus=fx_b_`x'+2*se_`x'
gen fx_`x'_2se_minus=fx_b_`x'-2*se_`x'

sort b_`x'
cap drop cid
gen cid=_n
labmask cid, values(curr)
summ b_`x'
local n_temp=r(N)
display "`x'_`time'"
local temp=upper("`x'")

twoway (scatter b_`x' cid if curr~="CNY" & em==1,mcolor(blue)) (rcap `x'_2se_minus `x'_2se_plus cid if curr~="CNY" & em==1,lcolor(blue)) (scatter b_`x' cid if curr~="CNY" & dm==1,mcolor(green)) (rcap `x'_2se_minus `x'_2se_plus cid if curr~="CNY" & dm==1,lcolor(green)) (scatter b_`x' cid if curr=="CNY",mcolor(red)) (rcap `x'_2se_minus `x'_2se_plus cid if curr=="CNY", lcolor(red)), yline(0, lpattern(dash) lcolor(grey)) xlabel(1(1)`n_temp',valuelabel angle(vertical)) xtitle("") legend(off) graphregion(color(white)) ytitle("{&beta}{subscript:i} on `temp'") name("`x'_`time'", replace) 
graph export "$gcap_data/output/appendix_figures/A_XII_`x'_`time'_`sample'.eps", replace
}



******************************************************************************************************
* OTHER TABLES
******************************************************************************************************


* Table A.II: Summary of Rankings for Alternative Estimations

local year "2020"
local run_type "baseline ust weighted excl_index intensive_mg alt_thr alt_aum alt_fc"

clear
foreach alt in `run_type' {
    * load file
    use "${gcap_data}/output/holdings_similarity/correlations_LC_Govt_currency_`year'_`alt'.dta", clear
    * define currency groupc
    qui{
    gen group_currency = "EM" if(inlist(currency,$emcurr1) | inlist(currency,$emcurr2) | inlist(currency,$emcurr3))
    replace group_currency = "DM" if(inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2) )
    replace group_currency = "CNY" if currency == "CNY"
    replace group_currency = "DM" if inlist(currency, "HKD")
    replace group_currency = "EM" if inlist(currency, "TWD", "HUF","SGD","EGP")
    assert !missing(group_currency)
    }
    * organize data for plotting
    drop if missing(corr_dm_nospec)
    duplicates drop

    keep currency corr_dm_nospec year group_currency

    gsort -corr_dm_nospec
    gen order = _n
    collapse (mean) order, by(group_currency)
    gen case = "`alt'"
    qui reshape wide order, i(case) j(group_currency, string)
    qui save "${gcap_data}/temp/ranking_`alt'.dta", replace    
}

local run_type "baseline ust weighted excl_index intensive_mg alt_thr alt_aum alt_fc"
clear
foreach alt in `run_type' {
    append using "${gcap_data}/temp/ranking_`alt'.dta"
}

replace case = "Baseline" if case=="baseline"
replace case = "(a) UST as Reference" if case=="ust"
replace case = "(b) Weighted by FC AUM" if case=="weighted"
replace case = "(c) Excluding Index Funds" if case=="excl_index"
replace case = "(d) Intensive Margin" if case=="intensive_mg"
replace case = "(e). Alternative Specialist Threshold" if case=="alt_thr"
replace case = "(f). Alternative Minimum FC AUM" if case=="alt_aum"
replace case = "(g). Alternative FC Definition" if case=="alt_fc"

format orderCNY	orderDM	orderEM %13.0fc
listtex using "${gcap_data}/output/appendix_figures/table_A_II_summary_ranking.tex", replace ///
            rstyle(tabular) ///
            head("\begin{tabular}{lccc}" ///
                 "\hline  \\" ///
                 "& \textbf{CNY Rank} & \textbf{Average DM Rank}  & \textbf{Average EM Rank} \tabularnewline" ///
                 "\hline " ///
                 "\hline ") ///
            foot("\hline" ///
                 "\end{tabular}")


cap log close