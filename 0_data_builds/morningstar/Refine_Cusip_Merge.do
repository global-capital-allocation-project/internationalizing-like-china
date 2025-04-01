* --------------------------------------------------------------------------------------------------
* refine_cusip_merge
*
* This file merges in security-level data from the CUSIP Global Services (CGS) master files into the
* holdings data.
* --------------------------------------------------------------------------------------------------


local year = `1'
display "HoldingDetail_`year'_m"
capture confirm file "$temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta"
if _rc==0 {
	use "$temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta", clear
    * Add internal currency field 
	mmerge cusip using "$temp/Internal/Internal_Currency.dta", uname(internal_) unmatch(m)
	replace currency_id=internal_currency_id if  _merge==3 & !missing(internal_currency_id)
	drop internal_currency_id _merge
	gen obs=_n
    * Add residency from issuer number 
    gen issuer_number = substr(cusip,1,6)
    mmerge issuer_number using "$gcap_data/output/cgs/temp/cgs_compact_complete.dta" , uname(cgs_) unmatch(m)
    drop issuer_number
    * Add other CGS variables
	mmerge cusip using "$gcap_data/output/cgs/allmaster_essentials.dta", uname(cgs_) unmatch(m) update
	cap tostring cgs_coupon, force replace
	replace coupon=cgs_coupon if _merge==3 & !missing(cgs_coupon)
	replace maturitydate=cgs_maturity if _merge==3 & !missing(cgs_maturity)
	drop cgs_issuer_num cgs_mat cgs_co cgs_cu cgs_isi
	gen cusip6 = substr(cusip,1,6)
	replace cusip6 = "" if cusip6=="000000"

	save "$temp/HoldingDetail/HoldingDetail_`year'_m_cusipmerge.dta", replace
	*capture rm "$temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta"
	}
else {
	display "File $temp/HoldingDetail/HoldingDetail_`year'_m_extid_merge.dta does not exist"
}


