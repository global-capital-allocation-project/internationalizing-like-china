******************************************************************************************************
* SETUP
******************************************************************************************************

* Read globals
qui do Project_globals.do

* Logs
cap log close
cap mkdir "${gcap_data}/rmb_replication/logs"
log using "${gcap_data}/rmb_replication/logs/entry_figures.log", replace


* Creating the directory to store the results: 
cap mkdir "${gcap_data}/output/paper_figures"
cap mkdir "${gcap_data}/output/appendix_figures"

ssc install carryforward
******************************************************************************************************
* Plotting figures related to Investors' Entry:
******************************************************************************************************

* PAPER FIGURE 2: STABLE AND FLIGHTY
use  "${gcap_data}/output/investor_entry/overseas_factset_full_smooth.dta", clear
gen month = mofd(entry_date)
format month %tm
drop if inlist(iso_cou,"CHN","HKG")
keep model month entry 
gen count=1
drop if model==""
collapse (sum) count, by(model month)
encode model, gen(mid)
tsset mid month
format month %tm
drop if missing(month)
tsfill, full
bysort mid: carryforward model, replace
gsort - month
bysort mid: carryforward model, replace
replace count=0 if count==.
summ month
local start=r(min)
gen cumulative=.
tsset mid month
replace cumulative=count if month==`start'
replace cumulative=l.cumulative+count if month>`start'
bysort model: egen total=sum(count)
gen cdf=cumulative/total
gen max = 1
gsort model month
gen moved_x = month + 1
format moved_x %tm
* saving file that generates figure (available in the replication package):
drop if model=="bank"
save "${gcap_data}/output/investor_entry/investor_entry_by_type_date.dta", replace

twoway (line cdf moved_x if model=="stable", lwidth(medthick)) ///
       (line cdf moved_x if model=="portfolio", lwidth(medthick)), /// 
    xline(`=monthly("2003m6", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2011m12", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2016m2", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2017m7", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2019m4", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
    xline(`=monthly("2020m9", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
    ttext(0.9 `=monthly("2003m11", "YM")' "opening" "QFII", orientation("vertical") size("small")) ///
    ttext(0.9 `=monthly("2012m4", "YM")' "RQFII", orientation("vertical") size("small")) ///
    ttext(0.85 `=monthly("2016m6", "YM")' "CIBM Direct", orientation("vertical") size("small")) ///
    ttext(0.85 `=monthly("2017m11", "YM")' "Bond Connect", orientation("vertical") size("small")) ///
    ttext(0.3 `=monthly("2019m8", "YM")' "Bloomberg", orientation("vertical") size("small")) ///
    ttext(0.3 `=monthly("2021m1", "YM")' "JPM GBI-EM", orientation("vertical") size("small")) ///
    legend(order(1 "Stable" 2 "Flighty") rows(1) region(lcolor(black))) ///
    ytitle("CDF") ///
    xtitle("")  graphregion(color(white)) plotregion(lcolor(black))
graph export "${gcap_data}/output/paper_figures/2_entry_into_domestic_market.eps", as(eps) replace

* APPENDIX FIGURE A_VI: ALL CATEGORIES
use  "${gcap_data}/output/investor_entry/overseas_factset_full_smooth.dta", clear
gen month = mofd(entry_date)
format month %tm
drop if inlist(iso_cou,"CHN","HKG")
keep broad entry_date
gen count=1
drop if broad==""
gen month=mofd(entry)
drop entry
collapse (sum) count, by(broad month)
encode broad, gen(mid)
count if missing(month)
drop if missing(month)
drop if missing(broad)
tsset mid month
format month %tm
tsfill, full
bysort mid: carryforward broad, replace
gsort - month
bysort mid: carryforward broad, replace

replace count=0 if count==.
summ month
local start=r(min)
gen cumulative=.
tsset mid month
replace cumulative=count if month==`start'
replace cumulative=l.cumulative+count if month>`start'
bysort broad: egen total=sum(count)
gen cdf=cumulative/total
keep broad month cdf
drop if missing(broad)
reshape wide cdf, i(month) j(broad) str
tsset month
foreach x of varlist cdf* {
    local temp="`x'"
    local temp2=proper(subinstr("`temp'","cdf","",.))
    local temp3=subinstr("`temp2'","_"," ",.)
    label var `x' "`temp3'"
}
label var cdfib "Investment Banks"

twoway (tsline cdf* , lpattern(solid solid solid solid solid solid solid solid solid)), /// 
        xline(`=monthly("2003m6", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
        xline(`=monthly("2011m12", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
        xline(`=monthly("2016m2", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
        xline(`=monthly("2017m7", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
        xline(`=monthly("2019m4", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
        xline(`=monthly("2020m9", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
        legend(size(small) col(3) region(lcolor(black))) ///
        ytitle("CDF") ///
        xtitle("") ///
        graphregion(color(white)) plotregion(lcolor(black))
graph export "${gcap_data}/output/appendix_figures/A_VI_entry_all_categories.eps", as(eps) replace

* APPENDIX FIGURE A.VI: PORTFOLIO MANAGEMENT AND INVESTMENT ADVICE INTO MUTUAL AND HEDGE FUNDS
* Using Factset for Classifying: 
* From entity to parent: 
use "${gcap_data}/input/factset/sym_entity.dta", clear
qui mmerge factset_entity_id using "${gcap_data}/input/factset/ent_entity_structure.dta", unmatched(m)
keep factset_entity_id factset_parent_entity_id entity_proper_name entity_type
replace factset_parent_entity_id = factset_entity_id if missing(factset_parent_entity_id)
qui mmerge factset_parent_entity_id using "${gcap_data}/input/factset/sym_entity.dta", ukeep(entity_type) unmatched(m) uname(parent_) umatch(factset_entity_id)
drop _merge
replace entity_type = "MUT" if regexm(entity_proper_name," ETF")
replace entity_type = "MUT" if entity_type == "MUE"
replace entity_type = "HED" if entity_type == "PVF"
replace parent_entity_type = "MUT" if parent_entity_type == "MUE"
replace parent_entity_type = "HED" if parent_entity_type == "PVF"
drop if entity_type =="EXT"
gen _temp = entity_type
replace _temp = "" if inlist(_temp,"EXT","PVT","SUB","HOL","PUB")
sort factset_parent_entity_id _temp
by factset_parent_entity_id: gen parentNval = _N
by factset_parent_entity_id _temp: gen parentNval_type = _N
replace parentNval_type = 0 if _temp == ""
gen parentShare = parentNval_type / parentNval
gen parent_entity_type_bg = parent_entity_type
gsort factset_parent_entity_id -parentShare
by factset_parent_entity_id: replace parent_entity_type_bg = entity_type[1] 
gsort factset_parent_entity_id
by factset_parent_entity_id: egen _temp_tot = total(parentShare)
replace parent_entity_type_bg = parent_entity_type if _temp_tot == 0
gen _temp_hed = 0
by factset_parent_entity_id: replace _temp_hed = 1 if entity_type == "HED"
by factset_parent_entity_id: egen count_hed = total(_temp_hed)
gen _temp_mut = 0
by factset_parent_entity_id: replace _temp_mut = 1 if entity_type == "MUT"
by factset_parent_entity_id: egen count_mut = total(_temp_mut)
gen shHED = count_hed/ parentNval
gen shMUT = count_mut/ parentNval
replace parent_entity_type_bg = "MUT" if shMUT > shHED & shMUT > 0
replace parent_entity_type_bg = "MUT" if shMUT == shHED & shMUT > 0
replace parent_entity_type_bg = "HED" if shHED > shMUT & shHED > 0
keep factset_parent_entity_id parent_entity_type_bg parent_entity_type
duplicates drop
count
count if parent_entity_type_bg != parent_entity_type
save "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_parent.dta", replace
* auxiliary file: 
use "${gcap_data}/input/factset/ent_entity_structure.dta", clear
replace factset_parent_entity_id = factset_entity_id if missing(factset_parent_entity_id)
keep factset_parent_entity_id factset_ultimate_parent_entity_i
drop if missing(factset_parent_entity_id) 
duplicates drop
save "${gcap_data}/output/investor_entry/temp/factset_entity_structure_crosswalk.dta", replace
* From Parent to Ultimate parent:
use "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_parent.dta", clear
mmerge factset_parent_entity_id using "${gcap_data}/output/investor_entry/temp/factset_entity_structure_crosswalk.dta", unmatched(m) ukeep(factset_ultimate_parent_entity_i)
drop _merge
replace factset_ultimate_parent_entity_i = factset_parent_entity_id if missing(factset_ultimate_parent_entity_i)
replace parent_entity_type_bg = "MUT" if parent_entity_type_bg == "MUE"
replace parent_entity_type_bg = "HED" if parent_entity_type_bg == "PVF"
replace parent_entity_type = "MUT" if parent_entity_type == "MUE"
replace parent_entity_type = "HED" if parent_entity_type == "PVF"
drop if parent_entity_type_bg =="EXT"
gen _temp = parent_entity_type_bg
replace _temp = "" if inlist(_temp,"EXT","PVT","SUB","HOL","PUB")
sort factset_ultimate_parent_entity_i _temp
by factset_ultimate_parent_entity_i: gen ultimateNval = _N
by factset_ultimate_parent_entity_i _temp: gen ultimateNval_type = _N
replace ultimateNval_type = 0 if _temp == ""
gen ultimateShare = ultimateNval_type / ultimateNval
gen ultimate_entity_type_bg = parent_entity_type_bg
gsort factset_ultimate_parent -ultimateShare
by factset_ultimate_parent_entity_i: replace ultimate_entity_type_bg = parent_entity_type_bg[1]
gsort factset_ultimate_parent_entity_i
by factset_ultimate_parent_entity_i: egen _temp_tot = total(ultimateShare)
replace ultimate_entity_type_bg = parent_entity_type_bg if _temp_tot == 0
sort factset_ultimate_parent_entity_i 
gen _temp_hed = 0
by factset_ultimate_parent_entity_i: replace _temp_hed = 1 if parent_entity_type_bg	 == "HED"
by factset_ultimate_parent_entity_i: egen count_hed = total(_temp_hed)
gen _temp_mut = 0
by factset_ultimate_parent_entity_i: replace _temp_mut = 1 if parent_entity_type_bg	 == "MUT"
by factset_ultimate_parent_entity_i: egen count_mut = total(_temp_mut)
gen shHED = count_hed/ ultimateNval
gen shMUT = count_mut/ ultimateNval
replace ultimate_entity_type_bg = "MUT" if shMUT > shHED & shMUT > 0
replace ultimate_entity_type_bg = "MUT" if shMUT == shHED & shMUT > 0
replace ultimate_entity_type_bg = "HED" if shHED > shMUT & shHED > 0
keep factset_ultimate_parent_entity_i ultimate_entity_type_bg
duplicates drop
drop if missing(ultimate_entity_type_bg)
save "${gcap_data}/output/investor_entry/temp/factset_type_parent_to_ultimate.dta", replace
* From entity to ultimate parent: 
use "${gcap_data}/input/factset/sym_entity.dta", clear
qui mmerge factset_entity_id using "${gcap_data}/input/factset/ent_entity_structure.dta", unmatched(m)
keep factset_entity_id factset_ultimate_parent_entity_i entity_proper_name entity_type
replace factset_ultimate_parent_entity_i = factset_entity_id if missing(factset_ultimate_parent_entity_i)
qui mmerge factset_ultimate_parent_entity_i using "${gcap_data}/input/factset/sym_entity.dta", ukeep(entity_type) unmatched(m) uname(ultimate_) umatch(factset_entity_id)
drop _merge
replace entity_type = "MUT" if regexm(entity_proper_name," ETF")
replace entity_type = "MUT" if entity_type == "MUE"
replace entity_type = "HED" if entity_type == "PVF"
drop if entity_type == "EXT"
replace factset_ultimate_parent_entity_i = "MUT" if factset_ultimate_parent_entity_i == "MUE"
replace factset_ultimate_parent_entity_i = "HED" if factset_ultimate_parent_entity_i == "PVF"
gen _temp = entity_type
replace _temp = "" if inlist(_temp,"EXT","PVT","SUB","HOL","PUB")
sort factset_ultimate_parent_entity_i _temp
by factset_ultimate_parent_entity_i: gen ultimateNval = _N
by factset_ultimate_parent_entity_i _temp: gen ultimateNval_type = _N
replace ultimateNval_type = 0 if _temp == ""
gen ultimateShare = ultimateNval_type / ultimateNval
gen ultimate_entity_type_bg = ultimate_entity_type
gsort factset_ultimate_parent_entity_i -ultimateShare
by factset_ultimate_parent_entity_i: replace ultimate_entity_type_bg = entity_type[1] 
gsort factset_ultimate_parent_entity_i
by factset_ultimate_parent_entity_i: egen _temp_tot = total(ultimateShare)
replace ultimate_entity_type_bg = ultimate_entity_type if _temp_tot == 0
gen _temp_hed = 0
by factset_ultimate_parent_entity_i: replace _temp_hed = 1 if entity_type == "HED"
by factset_ultimate_parent_entity_i: egen count_hed = total(_temp_hed)
gen _temp_mut = 0
by factset_ultimate_parent_entity_i: replace _temp_mut = 1 if entity_type == "MUT"
by factset_ultimate_parent_entity_i: egen count_mut = total(_temp_mut)
gen shHED = count_hed/ ultimateNval
gen shMUT = count_mut/ ultimateNval
replace ultimate_entity_type_bg = "MUT" if shMUT > shHED & shMUT > 0
replace ultimate_entity_type_bg = "MUT" if shMUT == shHED & shMUT > 0
replace ultimate_entity_type_bg = "HED" if shHED > shMUT & shHED > 0
keep factset_ultimate_parent_entity_i ultimate_entity_type_bg ultimate_entity_type
duplicates drop
save "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_ultimate.dta", replace
* Combining reclassifications: 
use "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_ultimate.dta", clear
rename factset_ultimate_parent_entity_i entity_id
rename ultimate_entity_type_bg ultimate_bg
drop ultimate_entity_type
mmerge entity_id using "${gcap_data}/output/investor_entry/temp/factset_type_parent_to_ultimate.dta", umatch(factset_ultimate_parent_entity_i)
drop _merge
rename ultimate_entity_type_bg ultimate_parent_bg
replace ultimate_parent = ultimate_bg if missing(ultimate_parent)
mmerge entity_id using "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_parent.dta", umatch(factset_parent_entity_id)
drop _merge
rename parent_entity_type_bg parent_bg
drop parent_entity_type
replace ultimate_bg = parent_bg if missing(ultimate_bg)
replace ultimate_parent_bg = parent_bg if missing(ultimate_parent_bg)
gen final_bg = ultimate_bg
replace final_bg = ultimate_parent_bg if missing(final_bg)
replace final_bg = parent_bg if missing(final_bg)
keep entity_id final_bg
drop if missing(entity_id)
duplicates drop
drop if missing(entity_id)
unique(entity_id)
save "${gcap_data}/output/investor_entry/temp/factset_entity_type_reclassified.dta", replace
* Final file: 
use  "${gcap_data}/output/investor_entry/overseas_factset_full_smooth.dta", clear
keep if inlist(broad, "investment_advice","portfolio_management") 
keep englishname iso_country factset_entity_id factset_ultimate_parent_entity_i 
duplicates drop
drop if missing(factset_entity_id) & missing(factset_ultimate_parent_entity_i)
drop if missing(factset_entity_id) & factset_ultimate_parent_entity_i == "xxx"
rename factset_entity_id entity_id
unique(entity_id)
duplicates drop iso_country	entity_id factset_ultimate_parent_entity_i, force
qui mmerge entity_id using "${gcap_data}/output/investor_entry/temp/factset_entity_type_reclassified.dta", unmatched(m)
qui mmerge factset_ultimate_parent_entity_i using "${gcap_data}/output/investor_entry/temp/factset_entity_type_reclassified.dta", unmatched(m) umatch(entity_id) uname(__)
replace final_bg = __final_bg if missing(final_bg)
drop if missing(entity_id)
duplicates drop
replace final_bg = "portfolio_OTH" if !inlist(final_bg,"MUT","HED")
save "${gcap_data}/output/investor_entry/temp/final_bg_portfolio.dta", replace
* Making the figure: 
use "${gcap_data}/output/investor_entry/overseas_factset_full_smooth.dta", clear
gen month = mofd(entry_date)
format month %tm
drop if inlist(iso_cou,"CHN","HKG")
qui mmerge factset_entity_id using "${gcap_data}/output/investor_entry/temp/final_bg_portfolio.dta", unmatched(m) umatch(entity_id)
replace model = final_bg if !missing(final_bg)
replace model = "OTH" if model == "portfolio"
keep model month entry 
gen count=1
drop if model==""
collapse (sum) count, by(model month)
encode model, gen(mid)
tsset mid month
format month %tm
drop if missing(month)
tsfill, full
bysort mid: carryforward model, replace
gsort - month
bysort mid: carryforward model, replace
replace count=0 if count==.
summ month
local start=r(min)
gen cumulative=.
tsset mid month
replace cumulative=count if month==`start'
replace cumulative=l.cumulative+count if month>`start'
bysort model: egen total=sum(count)
gen cdf=cumulative/total
gen max = 1
gsort model month
gen moved_x = month + 1
format moved_x %tm

twoway (line cdf moved_x if model=="stable", lwidth(medthick)) ///
       (line cdf moved_x if model=="MUT", lwidth(medthick) lcolor(dkorange) ) ///
       (line cdf moved_x if model=="HED", lwidth(medthick) lcolor(khaki)), /// 
    xline(`=monthly("2003m6", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2011m12", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2016m2", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2017m7", "YM")', lpattern("dash") lwidth("medthick") lcolor("blue")) ///
    xline(`=monthly("2019m4", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
    xline(`=monthly("2020m9", "YM")', lpattern("dash") lwidth("medthick") lcolor("red")) ///
    ttext(0.9 `=monthly("2003m11", "YM")' "opening" "QFII", orientation("vertical") size("small")) ///
    ttext(0.9 `=monthly("2012m4", "YM")' "RQFII", orientation("vertical") size("small")) ///
    ttext(0.85 `=monthly("2016m6", "YM")' "CIBM Direct", orientation("vertical") size("small")) ///
    ttext(0.85 `=monthly("2017m11", "YM")' "Bond Connect", orientation("vertical") size("small")) ///
    ttext(0.3 `=monthly("2019m8", "YM")' "Bloomberg", orientation("vertical") size("small")) ///
    ttext(0.3 `=monthly("2021m1", "YM")' "JPM GBI-EM", orientation("vertical") size("small")) ///
    legend(order(1 "Stable" 2 "Mutual Funds" 3 "Hedge Funds") rows(1) region(lcolor(black))) ///
    ytitle("CDF") ///
    xtitle("")  ///
    graphregion(color(white)) plotregion(lcolor(black))	
graph export "${gcap_data}/output/appendix_figures/A_VI_entry_mutual_hedge_funds.eps", as(eps) replace

* removing intermediary files:
cap rm "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_parent.dta"
cap rm "${gcap_data}/output/investor_entry/temp/factset_type_entity_to_ultimate.dta"
cap rm "${gcap_data}/output/investor_entry/temp/factset_type_parent_to_ultimate.dta"
cap rm "${gcap_data}/output/investor_entry/temp/final_bg_portfolio.dta"
cap rm "${gcap_data}/output/investor_entry/temp/factset_entity_type_reclassified.dta"
cap rm "${gcap_data}/output/investor_entry/temp/factset_entity_structure_crosswalk.dta"

cap log close
