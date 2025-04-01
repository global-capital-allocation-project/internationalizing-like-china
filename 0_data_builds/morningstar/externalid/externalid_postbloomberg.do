* --------------------------------------------------------------------------------------------------
* Merge_OpenFigi_BBG
*
* This job loads CSV output data obtained from OpenFIGI as well as the corresponding raw data pulled
* from the Bloomberg terminal. It merges these and produces a consolidated DTA file with information
* from the OpenFIGI pull.
*
* Second part of the job take the externalid_mns keyfile obtained from the OpenFIGI data pull and and checks
* whether the downloaded identifiers match to data in the CUSIP and ISIN master files produced by
* the present build. This then creates a linking file, so that at the start of the build process,
* we can match on externalid and keep ISIN and CUSIP.
*
* --------------------------------------------------------------------------------------------------

global raw_externalid = "${gcap_data}/input/morningstar/externalid"

* Load first keyfile 
clear
insheet using "${gcap_data}/output/morningstar/temp/externalid/externalid_keyfile.csv", names
foreach var of varlist exchcode-securitydescription {
	replace `var' = "" if `var'=="NA"
}
* Append full-external-id file (if exists). 
cap append using "$raw_externalid/externalid_keyfile.dta"
duplicates drop externalid_mns, force
order externalid_mns idformat
gen length = length(externalid_mns)
gsort length idformat
drop length
save "${gcap_data}/output/morningstar/temp/externalid/externalid_keyfile.dta", replace

* Load raw data from Bloomberg
clear
insheet using "$raw_externalid/bbg_figi.csv", names
* Next two lines are for safety
* In case somebody pulls in a duplicate record in a future BBG pull
duplicates drop
bysort figi: drop if _n>1
save "${gcap_data}/output/morningstar/temp/externalid/bbg_figi_data.dta", replace


* Join with raw data from OpenFIGI
merge 1:m figi using "${gcap_data}/output/morningstar/temp/externalid/externalid_keyfile.dta"
keep if _merge > 1
order externalid_mns idformat figi-_merge
drop _merge
duplicates drop
gen length = length(externalid_mns)
gsort length idformat externalid_mns
drop length

* Make consistent with MNS data
replace crncy = upper(crncy)
ren id_isin isin
ren id_cusip cusip
ren cntry_issue_iso iso2
ren crncy currency_id
ren cpn coupon
ren maturity maturitydate

* Fix coupon format
tostring coupon, replace force

* Fix iso_country_codes
replace iso2 = "XSN" if iso2 == "MULT" | iso2 == "SNAT"
mmerge iso2 using "${gcap_data}/input/miscellaneous/iso2_iso3.dta"
drop if _merge==2
replace iso3 = "XSN" if iso2 == "XSN"
drop _merge iso2
rename iso3 iso_country_code

* Fix maturitydate format
replace maturitydate = subinstr(maturitydate, "/", "-", .)
gen maturitydate_cln = date(maturitydate, "MDY", 2085)
format maturitydate_cln %d
drop maturitydate
rename maturitydate_cln maturitydate
recast long maturitydate

* Generate mns_class and mns_subclass according to openfigi & bloomberg data.
gen figi_mns_class = ""
gen figi_mns_subclass = ""

* Market Sector Comdty
replace figi_mns_class = "D" if marketsector=="Comdty"
replace figi_mns_subclass = "NC" if marketsector=="Comdty"

* Market Sector Corp
replace figi_mns_class = "B" if marketsector== "Corp"
replace figi_mns_subclass = "C" if marketsector== "Corp"

* Market Sector Curncy
replace figi_mns_class = "D" if marketsector== "Curncy"
replace figi_mns_subclass = "C" if marketsector== "Curncy"

* Market Sector Equity
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Common Stock"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Depositary Receipt"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype=="GDR"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "FUTURE"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "Mutual Fund"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "Option" & securitytype == "Equity Option"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Partnership Shares" & securitytype == "Ltd Part"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Partnership Shares" & securitytype == "MLP"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Preference" & securitytype == "Preference"
replace figi_mns_class = "E" if marketsector== "Equity" & (securitytype2== "REIT" | securitytype == "REIT")
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Right" & securitytype == "Right"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Unit" & securitytype == "Stapled Security"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Unit" & securitytype == "Unit"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "Warrant"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "ADR"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Closed-End Fund"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Hedge Fund"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "Common Stock"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity Option"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity WRT"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "ETP"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Fund of Funds"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "I.R. Swp WRT"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Index WRT"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Indx Fut WRT"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Open-End Fund"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Right"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FORWARD"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FUTURE"

replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "FUTURE"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "Option" & securitytype == "Equity Option"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "Warrant"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity Option"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "I.R. Swp WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Index WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Indx Fut WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Right"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FORWARD"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FUTURE"

replace figi_mns_class = "B" if figi=="BBG000L50ZL3" // single fix for a conflicting openfigi error
replace figi_mns_subclass = "C" if figi=="BBG000L50ZL3" // single fix for a conflicting openfigi error

* Market Sector Govt
replace figi_mns_class = "B" if marketsector== "Govt"
replace figi_mns_subclass = "S" if marketsector== "Govt"

* Market Sector Index
replace figi_mns_class = "D" if marketsector== "Index"
replace figi_mns_subclass = "NC" if marketsector== "Index"

* Market Sector Mortgage
replace figi_mns_class = "B" if marketsector== "Mtge"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "2ND LIEN" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Auto"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Card"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Home"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Other"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "Agncy ABS Home"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "Agncy ABS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "ABS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/HG" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/HG" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDO2" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(ABS)" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(CRP)" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(CRP)" & securitytype == "HB"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMBS" & securitytype == "Agncy CMBS"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMBS" & securitytype == "CMBS"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO FLT"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO INV"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO IO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO Other"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO PO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO Z"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO FLT"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO IO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CRE" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "HY" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "HY" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "IG" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "MEZZ" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "MML" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "Cadian"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 10yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 15yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 20yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 30yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS ARM"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "SBA Pool"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "RMBS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "SME" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "SME" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TBA" & securitytype == "MBS balloon"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TRP" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TRP/REIT" & securitytype == "CF"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO FLT"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO IO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO FLT"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO IO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO PO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO Z"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 10yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 15yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 20yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 30yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS ARM"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS balloon"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS Other"

* Market Sector Muni
replace figi_mns_class = "B" if marketsector== "Muni"
replace figi_mns_subclass = "LS" if marketsector== "Muni"

* Market Sector Pfd
replace figi_mns_class = "E" if marketsector== "Pfd"

* Save output
order externalid_mns figi cusip isin name currency_id maturitydate coupon iso_country_code figi_mns_class figi_mns_subclass
sort externalid_mns
save "${gcap_data}/output/morningstar/temp/externalid/externalid_openfigi_bloomberg.dta", replace


* Part 2 of the Job starts here:
use "${gcap_data}/output/morningstar/temp/externalid/externalid_keyfile.dta"
merge m:1 figi using "${gcap_data}/output/morningstar/temp/externalid/bbg_figi_data.dta", keepusing(id_cusip id_isin)
drop _merge

gen cusip = externalid_mns if idformat == "ID_CUSIP"
gen isin = externalid_mns if idformat == "ID_ISIN"
replace cusip = id_cusip if cusip == ""
replace isin = id_isin if isin == ""

keep externalid_mns idformat figi cusip isin

* Merge needs unique fields. Do not want to erroneously match something that's not a cusip or isin to one.
replace cusip = "unique" + externalid_mns if cusip == ""
replace isin = "unique" + externalid_mns if isin == ""
gen found_in_masters = ""


merge m:1 cusip using "${gcap_data}/output/cgs/allmaster_essentials.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, allmaster_essentials" if _merge==3 & found_in_masters==""
drop _merge
merge m:m isin using "${gcap_data}/output/cgs/allmaster_essentials.dta", keepusing(isin)
drop if _merge==2
replace found_in_masters = "isin, allmaster_essentials" if _merge==3 & found_in_masters==""
drop _merge
duplicates drop

rename cusip cusip_144a
merge m:m cusip_144a using"${gcap_data}/output/cgs/temp/ffaplusmaster.dta", keepusing(cusip_144a)
drop if _merge==2
replace found_in_masters = "cusip, ffaplusmaster" if _merge==3 & found_in_masters==""
drop _merge
rename cusip_144a cusip

merge m:1 cusip using "${gcap_data}/output/cgs/temp/incmstr.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, incmstr" if _merge==3 & found_in_masters==""
drop _merge
* Make sure ISINs are also uniquely identified after prioritizing CUSIPs, prioritize later years over earlier years
preserve
use "${gcap_data}/output/cgs/temp/incmstr.dta", clear
bysort isin (vintage) : keep if _n==_N
tempfile isin_merge
save `isin_merge'
restore
merge m:1 isin using `isin_merge', keepusing(isin)
drop if _merge==2
replace found_in_masters = "isin, incmstr" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/FHLMC.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, FHLMC" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/FNMA.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, FNMA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/GNMA.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, GNMA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/SBA.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, SBA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/TBA.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, TBA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using "${gcap_data}/output/cgs/temp/WorldBank.dta", keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, WorldBank" if _merge==3 & found_in_masters==""
drop _merge

gen splitat = strpos(found_in_masters,", ")
gen match_type = substr(found_in_masters,1,splitat - 1)
gen match_file = substr(found_in_masters,splitat + 2,.)
gen match_flag = !missing(match_type)
drop splitat found_in_masters
replace cusip = "" if regexm(cusip, "unique")
replace isin = "" if regexm(isin, "unique")

keep externalid_mns idformat figi match_type match_file cusip isin
drop if match_type==""
replace isin="" if match_type=="cusip"
replace cusip="" if match_type=="isin"
drop if missing(externalid_mns)

save "${gcap_data}/output/morningstar/temp/externalid/externalid_linking.dta", replace