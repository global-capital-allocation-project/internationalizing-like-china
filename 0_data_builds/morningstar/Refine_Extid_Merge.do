* --------------------------------------------------------------------------------------------------
* refine_extid_merge
*
* This job merges information from the internally-generated externalid master file into the holdings
* data. The resulting HoldingDetail files are referred to as "extid_merge" files.
* --------------------------------------------------------------------------------------------------

local year = `1'
local externid_to_keep "isin cusip iso_country_code currency_id coupon maturitydate mns_class mns_subclass"

use $temp/HoldingDetail/HoldingDetail_`year'_m_cusipfisin, clear

count
if `r(N)'>60000000 {
	keep if _n<=60000000
	mmerge externalid_mns using $temp/externalid/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
	drop if _merge==2
	foreach var in `externid_to_keep' {
		display "`var'"
		replace `var'=temp_`var' if missing(`var')==1
	}
	replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
	replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
	ren _merge _merge3		
	save $temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge, replace
	use $temp/HoldingDetail/HoldingDetail_`year'_m_cusipfisin, clear
	keep if _n>60000000
	mmerge externalid_mns using $temp/externalid/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
	drop if _merge==2
	foreach var in `externid_to_keep' {
		display "`var'"
		replace `var'=temp_`var' if missing(`var')==1
	}
	replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
	replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
	ren _merge _merge3		
	append using $temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge
	compress	
}
mmerge externalid_mns using $temp/externalid/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
drop if _merge==2
foreach var in `externid_to_keep' {
	display "`var'"
	replace `var'=temp_`var' if missing(`var')==1
}
replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
ren _merge _merge3
save "$temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta", replace
* capture rm "$temp/HoldingDetail/HoldingDetail_`year'_m_cusipfisin.dta"

