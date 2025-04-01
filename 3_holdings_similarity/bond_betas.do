******************************************************************************************************
* SETUP
******************************************************************************************************

* Read main path
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/bond_betas.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/bond_betas"
cap mkdir "${gcap_data}/output/bond_betas/temp"


******************************************************************************************************
* BOND BETAS
******************************************************************************************************

* USING THE DU IM SCHREGER DATA (JIE, 2018)
foreach freq in "month" "week" {
use "${gcap_data}/input/miscellaneous/dis_cip_all_adj_june2021.dta", clear

gen currency_tenor=currency+tenor
encode currency_tenor, gen(ctid)

mmerge date using "${gcap_data}/input/miscellaneous/vix.dta"
drop if _merge==2
if "`freq'"=="week" {
gen week=wofd(date)
format week %tw
}

if "`freq'"=="month" {
gen month=mofd(date)
format month %tm
}

collapse (lastnm) spot spot_norm y y_usd r r_usd xccy nds rho cip_govt vix, by(group currency tenor currency_tenor ctid `freq')
tsset ctid `freq'
gen n=tenor
replace n="0.25" if tenor=="3m"
replace n=subinstr(n,"y","",.)
destring n, replace
replace y=y/100 
replace y_usd=y_usd/100
gen p=-n*y
gen P=exp(p)


gen s_lcus=y-y_usd
gen rx_lcus=n*(l.s_lcus-s_lcus)
gen fx_change=log(spot_norm)-log(l.spot_norm)
gen fx_return=-fx_change

gen uhrx_lcus=rx_lcus+fx_return
gen hpr=p-l.p
gen hpr_usd=-n*y_usd-(-n*l.y_usd)
gen excess_hpr=hpr-hpr_usd+fx_return
gen d_log_vix=log(vix)-log(l.vix)


save "${gcap_data}/output/bond_betas/temp/dis_`freq'.dta", replace
}

*******************************************
*CARRY Trade regresions, quarter*
*******************************************

local time "quarter"
local scale=4

use "${gcap_data}/input/miscellaneous/dis_cip_all_adj_june2021.dta", clear
keep if tenor=="3m"

mmerge date using "${gcap_data}/input/miscellaneous/vix.dta"
gen month=mofd(date)
format month %tm
gen quarter=qofd(date)
format quarter %tq

collapse (lastnm) spot spot_norm y y_usd r r_usd xccy nds rho cip_govt vix, by(group currency `time')
encode currency, gen(cid)
tsset cid `time'

replace y=y/100 
replace y_usd=y_usd/100
gen contempidiff=(1/`scale')*(y-y_usd)

gen idiff=l1.contempidiff
drop contempidiff
gen fx_change=log(spot_norm)-log(l1.spot_norm)
gen fx_return=-fx_change
gen rx=idiff+fx_return

tsset cid `time'
sort cid `time'
keep if rx~=. & fx_return~=. & idiff~=.
cap drop n
bysort cid: gen n=_n
gen cum_rx=0 if n==1
gen cum_fx=0 if n==1
gen cum_carry=0 if n==1
replace cum_rx=cum_rx[_n-1]+rx if n>1
replace cum_fx=cum_fx[_n-1]+fx_return if n>1
replace cum_carry=cum_carry[_n-1]+idiff if n>1
order cum* rx fx_return n
gen d_log_vix=log(vix)-log(l.vix)
save "${gcap_data}/output/bond_betas/temp/carry_regs_`time'.dta", replace

*Create an HML
use "${gcap_data}/output/bond_betas/temp/carry_regs_`time'.dta", clear
drop if curr=="CNY"
gen fund=.
gen invest=.
summ `time'
local start=r(min)
local end=r(max)
forvalues x=`start'/`end' {
qui summ idiff if `time'==`x', detail
qui replace fund=1 if idiff<=r(p25) & `time'==`x'
qui replace invest=1 if idiff>=r(p75) & `time'==`x'
}

gen hml=-fund*rx
replace hml=invest*rx
drop if hml==.
collapse (mean) hml (firstnm) d_log_vix, by(`time')
save "${gcap_data}/output/bond_betas/temp/hml_`time'.dta", replace


local sample "post2010"
local time "quarter"

*CURRENCY BY CURRENCY
use "${gcap_data}/output/bond_betas/temp/carry_regs_`time'.dta", clear
mmerge `time' using "${gcap_data}/output/bond_betas/temp/hml_`time'.dta"
if "`sample'"=="post2010" {
if "`time'"=="quarter" {
	keep if quarter>=tq(2010q1)
}
}

drop vix
rename d_log_vix vix
gen b_hml=.
gen se_hml=.
gen b_vix=.
gen se_vix=.

gen alpha_hml=.
gen se_alpha_hml=.
gen alpha_vix=.
gen se_alpha_vix=.

gen fx_b_hml=.
gen fx_se_hml=.
gen fx_b_vix=.
gen fx_se_vix=.

levelsof(currency), local(curr)
foreach var in hml vix {
foreach x of local curr {
	qui reg rx `var' if curr=="`x'", r
	qui replace b_`var'=_b[`var'] if curr=="`x'"
	qui replace se_`var'=_se[`var'] if curr=="`x'"
	qui replace alpha_`var'=_b[_cons] if curr=="`x'"
	qui replace se_alpha_`var'=_se[_cons] if curr=="`x'"	
}
}

foreach var in hml vix {
foreach x of local curr {
	qui reg fx_return `var' if curr=="`x'", r
	qui replace fx_b_`var'=_b[`var'] if curr=="`x'"
	qui replace fx_se_`var'=_se[`var'] if curr=="`x'"
}
}

collapse (lastnm) b_* se_* fx_b* fx_se* alpha*, by(curr)

gen em=0
gen dm=0
replace em=1 if inlist(currency,$emcurr1) | inlist(currency,$emcurr2) | inlist(currency,$emcurr3)  
replace dm=1 if inlist(currency,$dmcurr1) | inlist(currency,$dmcurr2) 

save "${gcap_data}/output/bond_betas/hml_vix_full.dta", replace

cap log close