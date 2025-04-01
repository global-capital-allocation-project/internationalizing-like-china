* --------------------------------------------------------------------------------------------------
* Final_Clean_for_Analysis
*
* This job uses the final "HD" (Holding Detail) files to perform additional (project specific standardizations). 
* HD_for_Analysis files require the masterfiles created using all the data available to the GCAP project.
* --------------------------------------------------------------------------------------------------

local year = `1'


use "${gcap_data}/output/morningstar/output/HoldingDetail/HD_`year'_m_PreUnwind.dta", clear

drop if missing(isin) & missing(cusip)

mmerge cusip using "${gcap_data}/input/gcap/gcap_security_master_cusip.dta", type(n:1) missing(nomatch) unmatched(m) ukeep(isin) uname(master_)
mmerge isin using "${gcap_data}/input/gcap/gcap_security_master_isin.dta", type(n:1) missing(nomatch) unmatched(m) ukeep(cusip) uname(master_)

//replacing the missing ones
replace isin=master_isin if isin=="" & master_isin!=""
replace cusip=master_cusip if cusip=="" & master_cusip!=""
//if not in the correct format, drop it
gen _len_cusip = length(cusip)
replace cusip = "" if _len_cusip != 9
gen _len_isin = length(isin)
replace isin = "" if _len_isin != 12
drop if missing(isin) & missing(cusip)
drop _len*

//creating variables for going over the cases
gen has_isin = 0
replace has_isin = 1 if !missing(isin)
gen has_cusip = 0
replace has_cusip = 1 if !missing(cusip)
gen isin_match=0
replace isin_match=1 if isin==master_isin 
gen cusip_match=0
replace cusip_match=1 if cusip==master_cusip 
//case 2: has_cusip, no isin 
gen cusip_final = cusip if has_isin == 0 & has_cusip == 1 
gen isin_final = isin if has_isin == 0 & has_cusip == 1 

//case 3: has_isin, no cusip  
replace cusip_final = cusip if has_isin == 1 & has_cusip == 0
replace isin_final = isin if has_isin == 1 & has_cusip == 0 

//case 1D: has_isin, has_cusip, master_isin = isin, master_cusip = cusip -> MOST CASES!
replace isin_final = isin if cusip_match == 1 & isin_match == 1 & isin_match == 1 & cusip_match ==1
replace cusip_final = cusip if cusip_match == 1 & isin_match == 1 & isin_match == 1 & cusip_match ==1

//case 1B: has_isin, has_cusip, master_cusip matches, master_isin no 
replace isin_final = isin if has_isin == 1 & has_cusip == 1 & isin_match == 0 & cusip_match ==1
replace cusip_final = cusip if has_isin == 1 & has_cusip == 1 & isin_match == 0 & cusip_match ==1

//case 1C: has_isin, has_cusip, master_isin matches, master_cusip no 
replace isin_final = isin  if has_isin == 1 & has_cusip == 1 & isin_match == 1 & cusip_match ==0
replace cusip_final = cusip  if has_isin == 1 & has_cusip == 1 & isin_match == 1 & cusip_match ==0

//case 1A: has_isin, has_cusip, master_isin and master_cusip = 0 
preserve
keep if has_isin == 1 & has_cusip == 1 & isin_match == 0 & cusip_match ==0
keep isin cusip currency_id iso_country_code mns_class
cap duplicates drop
mmerge cusip using "${gcap_data}/input/gcap/gcap_security_master_cusip.dta", type(n:1) missing(nomatch) ukeep(isin class_code1 class_code2 class_code3 currency) uname(master_cu_) unmatched(m)
gen cusip6 = substr(cusip,1,6)
mmerge cusip6 using "${gcap_data}/input/cmns/cmns_aggregation.dta", type(n:1) missing(nomatch) umatch(issuer_num) ukeep(cgs_dom country_bg) uname(master_cu_) unmatched(m)
mmerge isin using "${gcap_data}/input/gcap/gcap_security_master_isin.dta", type(n:1) missing(nomatch) ukeep(cusip class_code1 class_code2 class_code3 currency) uname(master_is_) unmatched(m)
gen master_isin6 = substr(master_is_cusip,1,6) 
mmerge master_isin6 using "${gcap_data}/input/cmns/cmns_aggregation.dta", type(n:1) missing(nomatch) umatch(issuer_num) ukeep(cgs_dom country_bg) uname(master_is_) unmatched(m)
gen match_curr_is=0
gen match_curr_cu=0
gen match_iso_is=0
gen match_iso_cu=0
gen match_class_is=0
gen match_class_cu=0
replace match_curr_is=1 if currency_id==master_is_currency & !missing(currency_id) & !missing(master_is_currency)
replace match_curr_cu=1 if currency_id==master_cu_currency & !missing(currency_id) & !missing(master_cu_currency)
replace match_iso_is=1 if (iso_country==master_is_cgs_dom |  iso_country==master_is_country_bg) & !missing(iso_country) & !missing(master_is_cgs_dom)  & !missing(master_is_country_bg) 
replace match_iso_cu=1 if (iso_country==master_cu_cgs_dom |  iso_country==master_cu_country_bg) & !missing(iso_country) & !missing(master_cu_cgs_dom)  & !missing(master_cu_country_bg) 
*This mapping is imperfect, but useful. It was done based on the following information:
gen     temp1_mns_class="E" if mns_class=="E"
replace temp1_mns_class="B" if mns_class=="B"
replace temp1_mns_class="F" if mns_class=="MF"
replace temp1_mns_class="O" if mns_class=="A"
replace temp1_mns_class="C" if mns_class=="C"
replace temp1_mns_class="L" if mns_class=="L"
replace temp1_mns_class="D" if mns_class=="D"
replace temp1_mns_class="U" if mns_class=="Q"
replace match_class_is=1 if temp1_mns_class==master_is_class_code1 & !missing(temp1_mns_class) & !missing(master_is_class_code1)
replace match_class_cu=1 if temp1_mns_class==master_cu_class_code1 & !missing(temp1_mns_class) & !missing(master_cu_class_code1)
gen match_tot_is=0
gen match_tot_cu=0
replace match_tot_is = match_curr_is + match_iso_is + match_class_is
replace match_tot_cu = match_curr_cu + match_iso_cu + match_class_cu
gen pick_cusip =0
replace pick_cusip=1 if match_tot_cu>=match_tot_is
gen cusip_final = cusip if pick_cusip==1
gen isin_final = master_cu_isin if pick_cusip==1
replace cusip_final = master_is_cusip if pick_cusip==0
replace isin_final = isin if pick_cusip==0
keep isin isin_final cusip  cusip_final
duplicates drop
collapse (firstnm) isin_final cusip_final, by(isin cusip)
save $gcap_data/temp/temp_opt_`year'.dta, replace
restore

mmerge isin cusip using $gcap_data/temp/temp_opt_`year'.dta, uname(__)
replace isin_final = __isin_final if has_isin == 1 & has_cusip == 1 & isin_match == 0 & cusip_match==0 
replace cusip_final = __cusip_final if has_isin == 1 & has_cusip == 1 & isin_match == 0 & cusip_match==0 
 
drop __* _merge
rm $gcap_data/temp/temp_opt_`year'.dta
rename isin hd_isin
rename cusip hd_cusip
rename currency_id hd_currency_id
rename isin_final isin
rename cusip_final cusip

drop if missing(isin) & missing(cusip) //this is zero, just to guarantee
drop master_cusip master_isin has_isin has_cusip isin_match cusip_match cgs_domicile cusip6

// the final standardization: 
preserve
keep isin cusip marketvalue lcu_per_usd_eop
gen marketvalueusd = marketvalue / lcu_per_usd_eop
collapse (sum) marketvalueusd, by(cusip isin)
sort cusip
by cusip: egen _tot_cusip = total(marketvalueusd) if !missing(cusip)
by cusip: gen _Nval_cusip = _N if !missing(cusip)
sort isin
by isin: egen _tot_isin = total(marketvalueusd) if !missing(isin)
by isin: gen _Nval_isin = _N if !missing(isin)
gsort isin -marketvalueusd
by isin: replace cusip = cusip[1] if !missing(cusip[1]) & _Nval_isin > 1
drop if _Nval_isin == 2 & missing(cusip)
gsort cusip -marketvalueusd
by cusip: replace isin = isin[1] if !missing(isin[1]) & _Nval_cusip > 1
drop if _Nval_cusip == 2 & missing(isin)
keep cusip isin
duplicates drop
save $gcap_data/temp/temp_rm_`year'.dta, replace
restore
qui mmerge cusip using $gcap_data/temp/temp_rm_`year'.dta, uname(_)
replace isin = _isin if isin != _isin & !missing(cusip)
drop _isin
qui mmerge isin using $gcap_data/temp/temp_rm_`year'.dta, uname(_)
replace cusip = _cusip if cusip != _cusip & !missing(isin)
drop _cusip
rm $gcap_data/temp/temp_rm_`year'.dta

// for observations with cusip
gen cusip6 = substr(cusip,1,6)
mmerge cusip6 using "${gcap_data}/input/cmns/cmns_aggregation.dta", umatch(issuer_number) unmatched(m) ukeep(cgs_domicile country_bg)
//for observations without cusip
preserve
keep if missing(cusip6)
keep isin iso_country_code 
duplicates drop
bys isin: gen Nval = _N
gsort isin -iso_country_code
by isin: replace iso_country_code = iso_country_code[_n-1] if missing(iso_country_code)
drop Nval
duplicates drop
mmerge isin using "${gcap_data}/input/gcap/gcap_isin_to_factset_entity_id.dta", unmatched(m)
gen _temp_merge = _merge
mmerge factset_entity_id using "$gcap_data/input/gcap/gcap_factset_id_res_nat.dta", unmatched(m)

gsort isin -cgs_domicile
replace cgs_domicile = iso_country_code if _temp_merge == 1 & missing(cgs_domicile)
replace country_bg = iso_country_code if _temp_merge == 1 & missing(country_bg)
keep isin cgs_domicile country_bg _temp_merge
duplicates drop
gen registration_domain = substr(isin,1,2)
kountry registration_domain, from(iso2c) to(iso3c)
bys isin: gen Nval = _N
replace cgs_domicile = _ISO3C_ if Nval > 1 & !missing(_ISO3C_)
replace country_bg = _ISO3C_ if Nval > 1 & !missing(_ISO3C_)
keep isin cgs_domicile country_bg
duplicates drop
bys isin: keep if _n==1
keep isin cgs_domicile country_bg
save $gcap_data/temp/isin_no_cusip_countries_`year'.dta, replace
restore

mmerge isin using $gcap_data/temp/isin_no_cusip_countries_`year'.dta, unmatched(m) uname(_)
replace cgs_domicile = _cgs_domicile if missing(cgs_domicile) & missing(cusip6)
replace country_bg = _country_bg if missing(country_bg) & missing(cusip6)
drop _cgs_dom _country_bg

mmerge cusip using "${gcap_data}/input/gcap/gcap_security_master_cusip.dta", unmatched(m) ukeep(asset_class1 asset_class2 asset_class3 class_code1	class_code2	class_code3	currency maturity_date	issuance_date	coupon_percent)
mmerge isin using "${gcap_data}/input/gcap/gcap_security_master_isin.dta", unmatched(m) ukeep(asset_class1 asset_class2 asset_class3 class_code1	class_code2	class_code3	currency maturity_date	issuance_date	coupon_percent) uname(_)

local vars "asset_class1 asset_class2 asset_class3 class_code1	class_code2	class_code3	currency maturity_date	issuance_date	coupon_percent"
foreach var of varlist `vars' {
 replace `var' = _`var' if missing(`var')
}
drop _asset_class1	_asset_class2	_asset_class3	_class_code1	_class_code2	_class_code3	_currency	_maturity_date	_issuance_date	_coupon_percent

rm "$gcap_data/temp/isin_no_cusip_countries_`year'.dta"

replace currency = hd_currency_id if missing(currency)
replace cgs_domicile = iso_country_code if missing(cgs_domicile)
replace country_bg = iso_country_code if missing(country_bg)

mmerge mns_class mns_subclass using "${gcap_data}/input/gcap/typecde_mns_class_sec_master_unique.dta", uname(__) unmatched(m)
replace asset_class1 = __asset_class1 if missing(asset_class1)
replace asset_class2 = __asset_class1 if missing(asset_class2)
replace class_code1 = __class_code1 if missing(class_code1)
replace class_code2 = __class_code2 if missing(class_code2)
drop __*
drop _merge cgs_domicile_source
cap rename date_y year

cap drop if marketvalue ==0
cap drop if missing(marketvalue)
cap gen marketvalue_usd=marketvalue/lcu
cap order marketvalue marketvalue_usd
cap drop if marketvalue_usd==.
cap replace marketvalue_usd=marketvalue_usd/(10^9)

save "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_m_for_analysis.dta", replace
cap gen month = month(date)
keep if month==3 | month==6 | month==9 | month==12
cap gen quarter = quarter(date)
cap gen year = year(date)
cap gen date_q = yq(year,quarter)
format date_q %tq
cap drop month quarter year
compress
save "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_q_for_analysis.dta", replace
cap gen month = month(date)
cap keep if month==12
cap gen date_y = year(date)
format date_y %ty
cap drop month
compress
save "${gcap_data}/output/morningstar/output/HD_for_analysis/HD_`year'_y_for_analysis.dta", replace

cap log close
