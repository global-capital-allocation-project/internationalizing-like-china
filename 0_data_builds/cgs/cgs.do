* --------------------------------------------------------------------------------------------------
* CGS_Build
* --------------------------------------------------------------------------------------------------

global raw "${gcap_data}/input/cgs/unzip"
global output "${gcap_data}/output/cgs"
global temp "${gcap_data}/output/cgs/temp"

cap mkdir "$output"
cap mkdir "$temp"

* --------------------------------------------------------------------------------------------------
* INCMSTR file
* --------------------------------------------------------------------------------------------------

cap program drop build_incmstr
program build_incmstr
    rename v1 isin
    rename v2 issuer_num
    rename v3 issue_num
    rename v4 chk_digit
    rename v5 issuer_name
    rename v6 issue_desc
    rename v7 cfi_code
    rename v8 iso_domicile
    label var iso_domicile "ISO 2 Country code"
    rename v9 iso_currency
    rename v10 rate
    rename v11 maturity_date
    rename v12 last_modify_dt
    rename v13 status
    gen cusip = issuer_num+issue_num+chk_digit
    drop if cusip==""
    drop last_modify_dt
    duplicates drop
end

* INCMSTR, 2022 version from CGS
import delimited "$raw/nov2022/INCMSTR.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_incmstr
gen vintage = 22
save "$temp/incmstr_vintage_2022.dta" , replace

* INCMSTR, 2018 version from CGS
import delimited "$raw/nov2018/INCMSTR.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_incmstr
gen vintage = 18
save "$temp/incmstr_vintage_2018.dta" , replace

* INCMSTR, 2016 version from CGS
import delimited "$raw/dec2016/INCMSTR.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_incmstr
gen vintage = 16
save "$temp/incmstr_vintage_2016.dta" , replace

* Consolidate versions; we keep unique ISINs and in case of CUSIP conflicts we use the latest info
use "$temp/incmstr_vintage_2022.dta" , replace
append using "$temp/incmstr_vintage_2018.dta"
append using "$temp/incmstr_vintage_2016.dta"
bysort cusip (vintage) : keep if _n==_N
save "$temp/incmstr.dta" , replace

* --------------------------------------------------------------------------------------------------
* 144a file
* --------------------------------------------------------------------------------------------------

cap program drop build_ffaplusmaster
program build_ffaplusmaster
    rename v1 issuer_num
    rename v2 issuer_name
    rename v3 issuer_state_code
    rename v4 issuer_desc
    rename v5 maturity_date
    rename v6 rate
    rename v7 dated_date
    rename v8 link_to_issue
    rename v9 cusip_144a
    rename v10 entry_date
    rename v11 accredited_inv_cusip
    rename v12 accredited_entry_date
    rename v13 registered_cusip
    rename v14 registered_entry_date
    rename v15 reg_s
    rename v16 reg_s_entry_date
    rename v17 reg_s_isin
    rename v18 reg_s_update_date
    rename v19 issue_status
end

* 144a, 2022 version
import delimited "$raw/nov2022/FFAPlusMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_ffaplusmaster
gen vintage = 22
save "$temp/ffaplusmaster_vintage_2022.dta", replace

* 144a, 2018 version
import delimited "$raw/nov2018/FFAPlusMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_ffaplusmaster
gen vintage = 18
save "$temp/ffaplusmaster_vintage_2018.dta", replace

* 144a, 2016 version
import delimited "$raw/dec2016/FFAPlusMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_ffaplusmaster
gen vintage = 16
save "$temp/ffaplusmaster_vintage_2016.dta", replace

* Consolidate versions
use "$temp/ffaplusmaster_vintage_2022.dta", replace
append using "$temp/ffaplusmaster_vintage_2018.dta"
append using "$temp/ffaplusmaster_vintage_2016.dta"
bys cusip_144a (vintage) : keep if _n==_N
save "$temp/ffaplusmaster.dta", replace

* --------------------------------------------------------------------------------------------------
* Commercial paper files
* --------------------------------------------------------------------------------------------------

foreach fn in "CPMASTER_ATTRIBUTE" "CPMASTER_ISSUE" "CPMASTER_ISSUER" {
    import delimited "$raw/nov2018/`fn'.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
    local lower_fn = lower("`fn'")
    save "$temp/`lower_fn'.dta", replace
}

* Process file for commercial paper
use "$temp/cpmaster_issuer.dta", clear
rename v1 issuer_number
rename v2 issuer_check
rename v3 issuer_name
rename v11 issuer_type
rename v12 transaction_code
rename v15 state
rename v16 updated_date
drop v13 v14 v17
save "$temp/cpmaster_issuer_labeled.dta", replace

* --------------------------------------------------------------------------------------------------
* ALLMASTER_ISIN file
* --------------------------------------------------------------------------------------------------

* Common build steps for ALLMASTER_ISIN
cap program drop build_allmaster_isin
program build_allmaster_isin
	rename v1 ISSUER_NUM
	rename v2 ISSUE_NUM
	rename v3 ISSUE_CHECK
	rename v4 ISSUE_DESCRIPTION
	rename v5 ISSUE_ADDITIONAL_INFO
	rename v6 ISSUE_STATUS
	rename v7 ISSUE_TYPE_CODE
	rename v8 DATED_DATE
	rename v9 MATURITY_DATE
	rename v10 PARTIAL_MATURITY
	rename v11 COUPON_RATE
	cap tostring COUPON_RATE, replace force
	rename v12 CURRENCY_CODE
	rename v13 SECURITY_TYPE_DESCRIPTION
	rename v14 FISN
	rename v15 ISSUE_GROUP
	rename v16 ISIN
	rename v17 WHERE_TRADED
	rename v18 TICKER_SYMBOL
	rename v19 US_CFI_CODE
	rename v20 ISO_CFI_CODE
	rename v21 ISSUE_ENTRY_DATE
	rename v22 ALTERNATIVE_MINIMUM_TAX
	rename v23 BANK_QUALIFIED
	rename v24 CALLABLE
	rename v25 FIRST_COUPON_DATE
	rename v26 INITIAL_PUBLIC_OFFERING
	rename v27 PAYMENT_FREQUENCY_CODE
	rename v28 CLOSING_DATE
	rename v29 DEPOSITORY_ELIGIBLE
	rename v30 PRE_REFUNDED
	rename v31 REFUNDABLE
	rename v32 REMARKETED
	rename v33 SINKING_FUND
	rename v34 TAXABLE
	rename v35 BOND_FORM
	rename v36 ENHANCEMENTS
	rename v37 FUND_DISTRIBUTION_POLICY
	rename v38 FUND_INVESTMENT_POLICY
	rename v39 FUND_TYPE
	rename v40 GUARANTEE
	rename v41 INCOME_TYPE
	rename v42 INSURED_BY
	rename v43 OWNERSHIP_RESTRICTIONS
	rename v44 PAYMENT_STATUS
	rename v45 PREFERRED_TYPE
	rename v46 PUTABLE
	rename v47 RATE_TYPE
	rename v48 REDEMPTION
	rename v49 SOURCE_DOCUMENT
	rename v50 SPONSORING
	rename v51 VOTING_RIGHTS
	rename v52 WARRANT_ASSETS
	rename v53 WARRANT_STATUS
	rename v54 WARRANT_TYPE
	rename v55 UNDERWRITER
	rename v56 AUDITOR
	rename v57 PAYING_AGENT
	rename v58 TENDER_AGENT
	rename v59 TRANSFER_AGENT
	rename v60 BOND_COUNSEL
	rename v61 FINANCIAL_ADVISOR
	rename v62 MUNICIPAL_SALE_DATE
	rename v63 SALE_TYPE
	rename v64 OFFERING_AMOUNT
	rename v65 OFFERING_AMOUNT_CODE
	rename v66 ISSUE_TRANSACTION
	rename v67 ISSUE_LAST_UPDATE_DATE
	rename v68 RESERVED_1
	rename v69 RESERVED_2
	rename v70 RESERVED_3
	rename v71 RESERVED_4
	rename v72 RESERVED_5
	rename v73 RESERVED_6
	rename v74 RESERVED_7
	rename v75 RESERVED_8
	rename v76 RESERVED_9
	rename v77 RESERVED_10
	foreach x of varlist _all {
		local temp=lower("`x'")
		rename `x' `temp'
	}	
end

* ALLMASTER_ISIN, 2022 version
import delimited "$raw/nov2022/ALLCNPMASTER_ISIN.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_isin
gen vintage = 22
save "$temp/allmaster_isin_vintage_2022.dta", replace

* ACMD, 2022 version
* these files have the same format as ALLMASTER_ISIN
clear
local raw_files : dir "$raw/nov2022" files "ACMD*.PIP"
foreach file of local raw_files {
    local new_name = subinstr("`file'",".PIP",".dta",.)
    cap import delimited "$raw/nov2022/`file'", delimiter("|") encoding(ISO-8859-1) bindquote(nobind) varnames(nonames) clear
    if _rc!=0 di "ERROR for `file'"
    drop if v1=="999999" | missing(v1)
    gen source_file = "`file'"
    qui save "$temp/`new_name'", replace
}
* append ACMD files and generate versions unique at issuer and security levels
local acmd_files : dir "$temp" files "ACMD*.dta"
clear
foreach file of local acmd_files {
    cap append using "$temp/`file'" , force
}
gen vintage = 22
save "$temp/acmd_2022.dta" , replace

* ALLMASTER_ISIN, 2018 version
import delimited "$raw/nov2018/ALLMASTER_ISIN.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_isin
gen vintage = 18
save "$temp/allmaster_isin_vintage_2018.dta", replace

* ALLMASTER_ISIN, 2016 version
import delimited "$raw/dec2016/ALLMASTER_ISIN.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_isin
gen vintage = 16
save "$temp/allmaster_isin_vintage_2016.dta" , replace

* Consolidate versions
use "$temp/allmaster_isin_vintage_2022.dta", replace
tostring issue_transaction , force replace
cap drop reserved_*
append using "$temp/acmd_2022.dta" , force
cap drop reserved_*
append using "$temp/allmaster_isin_vintage_2018.dta" , force
cap drop reserved_*
append using "$temp/allmaster_isin_vintage_2016.dta" , force
cap drop reserved_*
tostring issue_check, force replace
gen cusip = issuer_num + issue_num + issue_check 
bys cusip (vintage) : keep if _n==_N
save "$output/allmaster_isin.dta", replace 

* --------------------------------------------------------------------------------------------------
* ALLMASTER_ISSUER file
* --------------------------------------------------------------------------------------------------

* Common build steps for ALLMASTER_ISSUER
cap program drop build_allmaster_issuer
program build_allmaster_issuer
	rename v1 ISSUER_NUMBER
	rename v2 ISSUER_CHECK
	rename v3 ISSUER_NAME
	rename v4 ISSUER_ADL
	rename v5 ISSUER_TYPE
	rename v6 ISSUER_STATUS
	rename v7 DOMICILE
	rename v8 STATE_CD
	rename v9 CABRE_ID
	rename v10 CABRE_STATUS
	rename v11 LEI_GMEI
	rename v12 LEGAL_ENTITY_NAME
	rename v13 PREVIOUS_NAME
	rename v14 ISSUER_ENTRY_DATE
	rename v15 CP_INSTITUTION_TYPE_DESC
	rename v16 ISSUER_TRANSACTION
	rename v17 ISSUER_UPDATE_DATE
	rename v18 RESERVED_1
	rename v19 RESERVED_2
	rename v20 RESERVED_3
	rename v21 RESERVED_4
	rename v22 RESERVED_5
	rename v23 RESERVED_6
	rename v24 RESERVED_7
	rename v25 RESERVED_8
	rename v26 RESERVED_9
	rename v27 RESERVED_10
	foreach x of varlist _all {
		local temp=lower("`x'")
		rename `x' `temp'
	}	
	mmerge domicile using "${gcap_data}/input/miscellaneous/iso2_iso3.dta", umatch(iso2)
	drop if _merge==2
	replace dom=iso3 if _merge==3
	drop _merge iso3
end


* ALLMASTER_ISSUER, 2022 version
import delimited "$raw/nov2022/ALLCNPMASTER_ISSUER.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_issuer
gen vintage = 22
save "$temp/allmaster_issuer_vintage_2022.dta" , replace

* ALLMASTER_ISSUER, 2018 version
import delimited "$raw/nov2018/ALLMASTER_ISSUER.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_issuer
gen vintage = 18
save"$temp/allmaster_issuer_vintage_2018.dta" , replace

* ALLMASTER_ISSUER, 2016 version
import delimited "$raw/dec2016/ALLMASTER_ISSUER.PIP" , delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_allmaster_issuer
gen vintage = 16
save"$temp/allmaster_issuer_vintage_2016.dta" , replace

* Consolidate versions
use "$temp/allmaster_issuer_vintage_2022.dta", replace
append using "$temp/allmaster_issuer_vintage_2018.dta" , force
append using "$temp/allmaster_issuer_vintage_2016.dta" , force
bys issuer_number (vintage) : keep if _n==_N
save "$temp/allmaster_issuer.dta", replace

* --------------------------------------------------------------------------------------------------
* Associated Issuers (AI) file
* --------------------------------------------------------------------------------------------------
* Common build steps for AI MASTER
cap program drop build_aimaster
program build_aimaster
cap drop v17
	rename v1 issuer_link
	rename v2 issuer_num
	rename v3 issuer_desc
	rename v4 action_type_1
	rename v5 new_name_1
	rename v6 effective_date_1
	rename v7 pending_flag
	rename v8 action_type_2
	rename v9 new_name_2
	rename v10 effective_date_2
	rename v11 action_type_3
	rename v12 new_name_3
	rename v13 effective_date_3
	rename v14 issuer_status
	rename v15 issuer_type
	rename v16 update_flag
end

* 2022 version
import delimited "$raw/nov2022/AIMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_aimaster
gen vintage = 22
save "$temp/aimaster_vintage_2022.dta" , replace

* 2018 version
import delimited "$raw/nov2018/AIMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_aimaster
gen vintage = 18
save "$temp/aimaster_vintage_2018.dta" , replace

* 2016 version
import delimited "$raw/dec2016/AIMASTER.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
build_aimaster
gen vintage = 16
save "$temp/aimaster_vintage_2016.dta" , replace

* Consolidate versions; if there are any conflicts we use the latest version of the file
use "$temp/aimaster_vintage_2022.dta" , replace
append using "$temp/aimaster_vintage_2018.dta"
append using "$temp/aimaster_vintage_2016.dta"
bys issuer_num issuer_link (vintage) : keep if _n==_N
duplicates drop issuer_num issuer_link, force
bysort issuer_num: egen max_file_vintage = max(vintage)
keep if vintage == max_file_vintage
save "$temp/aimaster.dta" , replace

* --------------------------------------------------------------------------------------------------
* LEI Plus file
* --------------------------------------------------------------------------------------------------

import delimited "$raw/nov2022/CBRLEIMSTR.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
rename v3 lei_gmei
rename v4 issuer_number
keep lei_gmei issuer_number
keep if ~missing(lei_gmei) & ~missing(issuer_number)
duplicates drop
gen vintage = 22
save "$temp/lei_plus_vintage_2022.dta" , replace

import delimited "$raw/nov2018/CBRLEIMSTR.PIP", delimiter("|") stringcols(_all) bindquote(nobind) encoding(ISO-8859-1) clear
rename v3 lei_gmei
rename v4 issuer_number
keep lei_gmei issuer_number
keep if ~missing(lei_gmei) & ~missing(issuer_number)
duplicates drop
gen vintage = 18
save "$temp/lei_plus_vintage_2018.dta" , replace

use "$temp/lei_plus_vintage_2022.dta" , clear 
append using "$temp/lei_plus_vintage_2018.dta"
save "$output/lei_plus_formerge.dta", replace

* --------------------------------------------------------------------------------------------------
* Mortgages, TBA securities, SVF
* --------------------------------------------------------------------------------------------------

* GNMA, part 1
import delimited "$raw/nov2018/master_20131211.GM", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="GNMA"
save "$temp/GNMA.p1.dta", replace

* GNMA, part 2
import delimited "$raw/nov2018/issue_20170912.GM", delimiter(comma) encoding(ISO-8859-1) clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="GNMA"
save "$temp/GNMA.p2.dta", replace
append using "$temp/GNMA.p1.dta"
duplicates drop
save "$temp/GNMA.dta", replace

* SBA, part 1
import delimited "$raw/nov2018/master_20100512.SB", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="SBA"
save "$temp/SBA.p1.dta", replace

* SBA, part 2
import delimited "$raw/nov2018/issue_20081208.SB", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="SBA"
save "$temp/SBA.p2.dta", replace
append using "$temp/SBA.p1.dta"
duplicates drop
save "$temp/SBA.dta", replace

* FNMA, part 1
import delimited "$raw/nov2018/master_20160809.FM", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="FNMA"
save "$temp/FNMA.p1.dta", replace

* FNMA, part 2
import delimited "$raw/nov2018/issue_20160809.FM", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="FNMA"
save "$temp/FNMA.p2.dta", replace
append using "$temp/FNMA.p1.dta"
duplicates drop
save "$temp/FNMA.dta", replace

* FHLMC, part 1
import delimited "$raw/nov2018/master_20160815.FD", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="FHLMC"
save "$temp/FHLMC.p1.dta", replace

* FHLMC, part 2
import delimited "$raw/nov2018/issue_upd_20180718.FD", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="FHLMC"
save "$temp/FHLMC.p2.dta", replace
append using "$temp/FHLMC.p1.dta"
duplicates drop
save "$temp/FHLMC.dta", replace

* WB, part 1
import delimited "$raw/nov2018/master_20061206.IB", delimiter(space) encoding(ISO-8859-1)clear
drop if v3~=""
keep v1
rename v1 cusip10
gen cusip=substr(cusip10,1,9)
drop cusip10
gen agency="WorldBank"
save "$temp/WorldBank.p1.dta", replace

* WB, part 2
import delimited "$raw/nov2018/issue_20061206.IB", delimiter(comma) encoding(ISO-8859-1)clear
tostring v3, force replace
gen cusip = v1+v2+v3
keep cusip
gen agency="WorldBank"
save "$temp/WorldBank.p2.dta", replace
append using "$temp/WorldBank.p1.dta"
duplicates drop
save "$temp/WorldBank.dta", replace

* TBA
import delimited "$raw/nov2018/TBA Master File - Sept 2012 Rev.txt", encoding(ISO-8859-1) clear
replace v2=trim(v2)
split(v2), p(" ")
keep v1 v21
rename v1 cusip
rename v21 agency
replace agency=agency+"_TBA"
save "$temp/TBA.dta", replace

* Append the agency files
clear
foreach x in "GNMA" "FNMA" "FHLMC" "TBA" "SBA" "WorldBank" {
    append using "$temp/`x'.dta"
}
save "$temp/agency.dta", replace

* --------------------------------------------------------------------------------------------------
* Essentials from the CUSIP/ISIN master file
* --------------------------------------------------------------------------------------------------

use iso_cfi_code isin issuer_num issue_num issue_check currency_code maturity_date coupon_rate using "$output/allmaster_isin.dta", clear
capture tostring issue_check, replace force
gen cusip=issuer_num+issue_num+ issue_check
drop issue_num issue_check
drop if issuer_num=="999999"
gen length=strlen(cusip)
drop if length~=9
drop length
save "$temp/allmaster_essentials_step1.dta", replace

use "$temp/allmaster_issuer.dta", clear
gen length=strlen(issuer_num)
drop if length~=6
drop length
save "$temp/allmaster_issuer_merge.dta", replace

use "$temp/allmaster_essentials_step1.dta", clear
mmerge issuer_num using "$temp/allmaster_issuer_merge.dta", ukeep(domicile issuer_type) umatch(issuer_number)
keep if _merge==3
drop _merge
gen maturity2=date(maturity,"YMD")
format maturity2 %td
drop maturity_date
save "$output/allmaster_essentials.dta", replace

* --------------------------------------------------------------------------------------------------
* Append all the above
* --------------------------------------------------------------------------------------------------

* Format for appending
use "$temp/agency.dta", clear
gen domicile="USA"
gen currency_code="USD"
replace domicile="XSN" if agency=="WorldBank"
replace currency_code="" if agency=="WorldBank"
replace agency="" if agency=="WorldBank"
cap drop cusip10
gen source = "agency"
save  "$temp/agency_format.dta", replace

use "$output/allmaster_essentials.dta", clear
gen source = "allmaster"
mmerge cusip using "$temp/agency_format.dta"
replace source = "allmaster agency" if _merge==3 
drop _merge
save "$temp/allmaster_essentials_m.dta", replace

use "$temp/allmaster_essentials_m.dta", clear
drop if isin==""
gen counter = 1 if !missing(isin)
bysort isin: egen count=sum(counter)
drop counter
drop if count~=1
save "$output/allmaster_essentials_isin.dta", replace


* Full appended file with CUSIP
* Cusips from allmaster_isin
use cusip source using "$temp/allmaster_essentials_m.dta", clear
save "$temp/all_cusip_p1.dta", replace
* Cusips from ffaplus
use cusip_144 using "$temp/ffaplusmaster.dta", clear
rename cusip_144 cusip
gen source = "ffaplus"
save "$temp/all_cusip_p2.dta", replace
* Cusips from incmstr
use cusip using "$temp/incmstr.dta", clear
gen source = "incmstr"
save "$temp/all_cusip_p3.dta", replace

* Combine files
use "$temp/all_cusip_p1.dta", clear
append using "$temp/all_cusip_p2.dta"
append using "$temp/all_cusip_p3.dta"
duplicates drop
* Track sources
bys cusip (source) : gen counter = _n
reshape wide source , i(cusip) j(counter)
gen cusip9_source = source1 + " " + source2 + " " + source3
drop source1 source2 source3
replace cusip9_source = subinstr(strtrim(cusip9_source),"  "," ",.)
gen issuer_number = substr(cusip, 1, 6)
drop if missing(issuer_number)
save "$temp/all_cusips_universe", replace

* Create version of CGS issuer master file with only issuer_num domicile issuer_name
use issuer_number dom issuer_name using "$temp/allmaster_issuer.dta", clear
drop if issuer_num==""
replace dom="ANT" if dom=="AN"
replace dom="SRB" if dom=="CS"
replace dom="FXX" if dom=="FX"
replace dom="XSN" if dom=="S2"
replace dom="XSN" if dom=="XS"
replace dom="YUG" if dom=="YU"
replace dom="ZAR" if dom=="ZR"
duplicates drop issuer_num dom, force
save "$temp/allmaster_issuer_compact.dta", replace

* Append agencies, TBAs, and World Bank
clear
foreach x in GNMA SBA GNMA FHLMC TBA WorldBank {
    append using "$temp/`x'.dta"
}
gen cusip6 = substr(cusip,1,6)
keep cusip6 agency
rename cusip6 issuer_number
rename agency issuer_name
duplicates drop
gen domicile="USA"
replace dom="XSN" if issuer_name=="WorldBank"
save "$temp/CGS_additional.dta", replace

* Prep file for 144A
use issuer_num issuer_name using "$temp/ffaplusmaster.dta", clear
gen domicile="USA"
rename issuer_num issuer_number
duplicates drop
save "$temp/ffaplusmaster_compact.dta", replace

* Prep file for commercial paper
use  "$temp/cpmaster_issuer_labeled.dta", clear
keep issuer_num issuer_name 
gen domicile="USA"
duplicates drop
save "$temp/cpmaster_issuer_compact.dta", replace

* Prep file for INCMSTR
use issuer_num issuer_name iso_domicile using "$temp/incmstr.dta", clear
rename issuer_num issuer_number
rename iso_dom domicile
duplicates drop
mmerge issuer_num using "$temp/allmaster_issuer_compact.dta"
keep if _merge==1
drop _merge
mmerge dom using "${gcap_data}/input/miscellaneous/iso2_iso3.dta", umatch(iso2) ukeep(iso3)
replace iso3="ANT" if dom=="AN"
drop if _merge==2
drop _merge dom
rename iso3 domicile
duplicates drop issuer_num dom, force
gen counter=1 if !missing(issuer_num)
bysort issuer_num: egen count=sum(counter)
gen rand=runiform()
bysort issuer_num: egen rand_max=max(rand)
drop if rand<rand_max & count>1
drop rand rand_max counter count
save "$temp/incmstr_additional_issuers.dta", replace

* Program to track source of data in master files
cap program drop track_source
program define track_source
    foreach var in issuer_name domicile {
        replace `var'_source = `var'_source + " `1'" if `var'==add_`var' & !missing(`var')
    }
end

* Append incrementally all above files (in order of priority of info quality)
* Start with allmaster
use "$temp/allmaster_issuer_compact.dta", clear
gen issuer_name_source = "allmaster" if !missing(issuer_name)
gen domicile_source = "allmaster" if !missing(domicile)
* Add agency
mmerge issuer_num using	"$temp/CGS_additional.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
track_source agency
drop add_* _merge
* Add incmstr
mmerge issuer_num using "$temp/incmstr_additional_issuers.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
track_source incmstr
drop add_* _merge
* Add ffaplus
mmerge issuer_num using	"$temp/ffaplusmaster_compact.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
track_source ffaplus
drop add_* _merge
* Add cpmaster
mmerge issuer_num using	"$temp/cpmaster_issuer_compact.dta", uname(add_)
replace issuer_name=add_issuer_name if _merge==2
replace domicile=add_domicile if _merge==2
track_source cpmaster
drop add_* _merge
duplicates drop 
unique(issuer_num)
save "$temp/cgs_compact_complete.dta", replace

* Also append with full security universe
use "$temp/cgs_compact_complete.dta", clear
drop if issuer_number == ""
duplicates drop issuer_number, force
mmerge issuer_number using "$temp/all_cusips_universe", unmatched(b)
replace cusip9_source = "none" if _merge==1
replace issuer_name_source = "missing" if missing(issuer_name)
replace domicile_source = "missing" if missing(domicile)
replace cusip = issuer_number + "XXX" if missing(cusip)
duplicates drop cusip, force
drop _merge
order cusip issuer_number issuer_name domicile cusip9_source issuer_name_source domicile_source
save "$output/all_cusips_universe_all_issuers", replace

* --------------------------------------------------------------------------------------------------
* ISIN to CUSIP mapping file
* --------------------------------------------------------------------------------------------------

use issuer_num issue_num issue_check isin curr using "$output/allmaster_isin.dta", clear
cap tostring issue_check, replace
gen cusip9 = issuer_num+issue_num+issue_check
drop issuer_num issue_num issue_check
drop if isin==""
rename curr cgs_currency
keep isin cusip9 cgs_currency
drop if missing(isin) | missing(cusip9)
bysort isin: keep if _n == 1
save "$output/isin_to_cusip.dta" , replace

use issuer_num issue_num issue_check isin curr using "$output/allmaster_isin.dta", clear
cap tostring issue_check, replace
gen cusip9 = issuer_num+issue_num+issue_check
drop issuer_num issue_num issue_check
drop if isin==""
rename curr cgs_currency
keep isin cusip9 cgs_currency
drop if missing(isin) | missing(cusip9)
bysort cusip: keep if _n == 1
save "$output/cusip_to_isin.dta" , replace

* --------------------------------------------------------------------------------------------------
* Compact version of the Associated Issuers file
* --------------------------------------------------------------------------------------------------

* Create the AI file for use in aggregation
use issuer_link issuer_num action_type_1 using "$temp/aimaster.dta", clear
rename issuer_num issuer_number
drop if issuer_link=="" | issuer_num==""
drop if issuer_link==issuer_num
drop if regexm(action_type_1,"Copyright 2016")==1
bysort issuer_num: gen n=_n
drop if n>1
drop n action
rename issuer_link ai_parent_issuer_num
mmerge ai_parent_issuer_num using "$temp/cgs_compact_complete.dta", umatch(issuer_num) uname("ai_parent_")
drop if _merge==2
drop _merge
keep ai_parent_issuer_num issuer_number ai_parent_issuer_name ai_parent_domicile
save "$temp/cgs_ai_aggregation.dta", replace
rename ai_parent_issuer_num ai_cusip6
rename issuer_number cusip6
rename ai_parent_issuer_name ai_name
rename ai_parent_domicile ai_residency
save "$output/cgs_ai_aggregation.dta", replace

* Removing all temporary files
cap rm "$output/cgs/temp"