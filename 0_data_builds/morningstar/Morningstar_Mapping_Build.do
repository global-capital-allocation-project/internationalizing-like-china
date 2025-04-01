* --------------------------------------------------------------------------------------------------
* Morningstar_Mapping_Build
*
* This do file creates crosswalk that connects InvestmentProductId to MasterPortfolioId based on three different versions of mapping files directly delivered by Morningstar at different time.
* (1) the oldest mapping file, "Mapping_Booth_20181220.xlsx", which is delivered in 2018
* (2) "BoothUniverse.xlsx", which is delivered in 2020
* (3) the latest mapping files which is delived in 2021 and include three mapping files ("GlobalFOBoothMap.csv", "NonFOGlobalBoothMap.csv" and "GlobalDeadBoothMap.csv")
* By integrating the above three versions of mappings files, this do file creates two versions of crosswalks (full and uniqueonly).
* --------------------------------------------------------------------------------------------------

* Hard coded years because of data availability
global firstyear = 2014
global lastyear = 2020

/* PROCESS FACTSET DATA FOR FO/FE APPORTIONING */

cap program drop parse_factset_aum
program parse_factset_aum
	drop if _n < 3
	local oldnames ""
	local newnames ""
	foreach var of varlist * {
	    local label : variable label `var'
	    if ("`label'" != "") {
	        local oldnames `oldnames' `var'
	        local newnames `newnames' _`label'_
	    }
	}
	rename (`oldnames') (`newnames')
	rename A date
	cap drop AJ
	gen year = year(date)
	drop date
	order year
	drop if missing(year)
	drop if year > $lastyear | year < $firstyear
	destring _*, force replace
	gen anchor = 1
	reshape wide _*, j(year) i(anchor)
	foreach var of varlist * {
	    local label : variable label `var'
	    if ("`label'" != "") {
	        local oldnames `oldnames' `var'
	        local newnames `newnames' _`label'_
	    }
	}
	foreach i of num $firstyear/$lastyear {
		rename _*_`i' y`i'_*
	}
	local stubs ""
	foreach i of num $firstyear/$lastyear {
		local stubs "`stubs' y`i'_"
	}
	reshape long `stubs', i(anchor) j(MasterPortfolioId)
	drop anchor
	rename *_ *
end

* get system dependent path
local etf_path = "$gcap_data/input/factset/etf_aum/latest"

* Load ETF AUM
import excel using `etf_path'/factset_etf_aum_updated.xlsx, clear sheet("ETF AUM") firstrow
quietly parse_factset_aum
save "$temp/Morningstar_Mapping_Build/etf_aum.dta", replace emptyok

* Load total fund AUM
import excel using `etf_path'/factset_etf_aum_updated.xlsx, clear sheet("Total Fund AUM") firstrow
parse_factset_aum
save "$temp/Morningstar_Mapping_Build/total_fund_aum.dta", replace emptyok

* Construct weights; we assign to FO by default if data is missing
use "$temp/Morningstar_Mapping_Build/total_fund_aum.dta", clear
mmerge MasterPortfolioId using "$temp/Morningstar_Mapping_Build/etf_aum.dta", unmatched(m) uname(e_)
foreach i of num $firstyear/$lastyear {
	gen etf_weight_`i' = min(1, e_y`i' / y`i')
	replace etf_weight_`i' = 0 if missing(e_y`i') | missing(y`i')
}
keep MasterPortfolioId etf_weight_*
save "$temp/Morningstar_Mapping_Build/etf_weights.dta", replace emptyok

/* Load and clean mapping file from morningstar direct to create a crosswalk with full masterportfolioid and investmentproductid */

*  Load and clean Mapping_Booth_20181220.xlsx file (delivered in 2018)
clear all
set excelxlsxlargefile on
local sheet_names ""Active USA" "nonActive USA" "Active NonUS" "NonActive NonUS""

foreach sheet of local sheet_names {
	preserve
	import excel using "$dir_mstar_raw/mapping/2018/Mapping_Booth_20181220.xlsx" , sheet("`sheet'") first clear
	count
	save temp, replace
	restore
	append using temp, force
	count
}
rm temp.dta // remove temporary .dta file
drop if missing(InvestmentIdName) & missing(SecId )
destring MasterPortfolioId ExchangeTradedShare ConvertedFundAUM ProspectusNetExpenseRatio ProspectusOperatingExpenseRatio AnnualReportGrossExpenseRatio, replace force
gen region="US" if Domicile=="United States"
replace region="Rest" if Domicile!="United States"
rename (SecId BroadCategoryName) (InvestmentProductId BroadCategoryGroup)
save "$temp/Morningstar_Mapping_Build/boothmap_2018", replace emptyok

* Load and clean BoothUniverse.xlsx file (delivered in 2020)
import excel using "$dir_mstar_raw/mapping/2020/BoothUniverse.xlsx", sheet("FE_ALL") firstrow cellrange(A1:K9702) clear
save "$temp/Morningstar_Mapping_Build/boothmap_2020_FE.dta", replace

import excel using "$dir_mstar_raw/mapping/2020/BoothUniverse.xlsx", sheet("FM_ALL") firstrow cellrange(A1:K6306) clear
save "$temp/Morningstar_Mapping_Build/boothmap_2020_FM.dta", replace

import excel using "$dir_mstar_raw/mapping/2020/BoothUniverse.xlsx", sheet("FO_ALL") firstrow cellrange(A1:K453251) clear
save "$temp/Morningstar_Mapping_Build/boothmap_2020_FO.dta", replace

clear
foreach fundtype in "FE" "FM" "FO" {
	capture append using "$temp/Morningstar_Mapping_Build/boothmap_2020_`fundtype'.dta"
	capture rm "$temp/Morningstar_Mapping_Build/boothmap_2020_`fundtype'.dta"
}
gen region="US" if DomicileCountryId=="USA"
replace region="Rest" if DomicileCountryId !="USA"
save "$temp/Morningstar_Mapping_Build/boothmap_2020.dta", replace

* Load and clean the three new mapping files (delivered in 2021)
import delimited using "$dir_mstar_raw/mapping/2021/GlobalFOBoothMap.csv", clear
save "$temp/Morningstar_Mapping_Build/GlobalFOBoothMap.dta", replace

import delimited using "$dir_mstar_raw/mapping/2021/GlobalDeadBoothMap.csv", clear
save "$temp/Morningstar_Mapping_Build/GlobalDeadBoothMap.dta", replace

import delimited using "$dir_mstar_raw/mapping/2021/NonFOGlobalBoothMap.csv", clear
save "$temp/Morningstar_Mapping_Build/NonFOGlobalBoothMap.dta", replace

use "$temp/Morningstar_Mapping_Build/GlobalFOBoothMap.dta", clear
capture append using "$temp/Morningstar_Mapping_Build/GlobalDeadBoothMap.dta"
capture append using "$temp/Morningstar_Mapping_Build/NonFOGlobalBoothMap.dta"
rename (masterportfolioid secid fundid status obsoletedate obsoletetype fundlegalstructure) (MasterPortfolioId InvestmentProductId FundId Status_new ObsoleteDate ObsoleteType FundLegalStructure)
replace Status_new="Inactive" if Status_new=="Obsolete"
drop id
sort MasterPortfolioId InvestmentProductId FundLegalStructure
bys MasterPortfolioId InvestmentProductId: replace FundLegalStructure = FundLegalStructure[_N]
duplicates drop
save "$temp/Morningstar_Mapping_Build/boothmap_2021.dta", replace
capture rm "$temp/Morningstar_Mapping_Build/GlobalFOBoothMap.dta"
capture rm "$temp/Morningstar_Mapping_Build/GlobalDeadBoothMap.dta"
capture rm "$temp/Morningstar_Mapping_Build/NonFOGlobalBoothMap.dta"


* Temporary Mapping file
use "$temp/Morningstar_Mapping_Build/boothmap_2021.dta", clear
gen file = "BoothMap(2021)"
merge 1:1 MasterPortfolioId InvestmentProductId using "$temp/Morningstar_Mapping_Build/boothmap_2020.dta", nogen
replace file = "BoothMap(2020)" if missing(file)
merge 1:1 MasterPortfolioId InvestmentProductId using "$temp/Morningstar_Mapping_Build/boothmap_2018.dta", nogen
replace file = "BoothMap(2018)" if missing(file)
drop if missing(MasterPortfolioId)
duplicates tag (InvestmentProductId), generate(dup)
sort InvestmentProductId file
bys InvestmentProductId: replace LegalType = LegalType[_N-1] if missing(LegalType[_N]) & dup > 0
bys InvestmentProductId: replace LegalType = LegalType[_N-2] if missing(LegalType[_N]) & dup > 0
bys InvestmentProductId: keep if _n == _N
gen DomicileCountryId2 = substr(domicilecountryid, 8, 10)
replace DomicileCountryId2 = substr(DomicileId, 8, 10) if missing(DomicileCountryId2)
replace DomicileCountryId = DomicileCountryId2 if missing(DomicileCountryId)
replace region = "US" if DomicileCountryId == "USA"
replace region = "Rest" if DomicileCountryId != "USA"
replace CurrencyId = substr(CurrencyId, 8, 10)
replace Status_new = "Inactive" if missing(Status_new) & Status == 0
replace Status_new = "Active" if missing(Status_new) & Status == 1
keep MasterPortfolioId InvestmentProductId FundId Status_new LegalType DomicileCountryId region CUSIP ISIN Ticker CurrencyId FundName ObsoleteDate ObsoleteType FundLegalStructure BroadCategoryGroup file
rename Status_new Status
rename MasterPortfolioId MasterPortfolioId_mapping
save "$output/Morningstar_Mapping_Build/boothmapping_full.dta", replace

/* Load and clean mapping file from morningstar direct to create a crosswalk with unique masterportfolioid */

use "$output/Morningstar_Mapping_Build/boothmapping_full.dta", clear
rename MasterPortfolioId_mapping MasterPortfolioId
keep MasterPortfolioId FundName DomicileCountryId Status BroadCategoryGroup LegalType
drop if missing(MasterPortfolioId)
bysort MasterPortfolioId : egen numDom = nvals(DomicileCountryId)
gen preferentialDom = DomicileCountryId
gen allDom = DomicileCountryId
bysort MasterPortfolioId (DomicileCountryId): replace allDom = allDom + ", " + DomicileCountryId[_n-1] if _n>1 & DomicileCountryId != DomicileCountryId[_n-1] & !missing(DomicileCountryId[_n-1])
bysort MasterPortfolioId (DomicileCountryId): replace allDom = allDom[_n-1] if allDom[_n-1] != allDom[_n] & strpos(allDom[_n-1], DomicileCountryId) > 0
bysort MasterPortfolioId (DomicileCountryId): replace allDom = allDom[_N]


/* Resolve duplicates of (DomicileCountryId, Status, LegalType, BroadCategoryGroup) within MasterPortfolioId */
* Preference ordering for multiple domiciles
gen resolved_conflict = .
foreach dom in "USA" "GBR" "IRL" "PRT" "LUX" "AUS" "SGP" "SWE" "FIN" "CHN" "HKG" "CYM" "BMU" "GGY" "VGB" "IMN" "CUW" "JEY" "MLT" "MUS" "PAN" {
	replace preferential = "`dom'" if strpos(allDom, "`dom'") & numDom > 1 & resolved_conflict==.
	replace resolved_conflict = 1 if strpos(allDom, "`dom'") & numDom > 1 & resolved_conflict==.
}
assert resolved_conflict == 1 if numDom > 1 & !missing(numDom)
replace DomicileCountryId = preferential
drop resolved_conflict preferential allDom numDom

* MasterPortfolioId is active if any are active
gen status = 1 if Status == "Active"
replace status = 0 if Status == "Inactive"
bysort MasterPortfolioId : egen maxstatus = max(status)
replace status = maxstatus
replace Status = "Active" if status == 1
replace Status = "Inactive" if status == 0
drop maxstatus status
duplicates drop

* MasterPortfolioId takes modal BroadCategoryGroup
bysort MasterPortfolioId: egen modalBroad = mode(BroadCategoryGroup), maxmode
replace BroadCategoryGroup = modalBroad
drop modalBroad
duplicates drop

* We keep only ONE record per MasterPortfolioId / LegalType combination; note that this mapping data
* does not carry any share-class specific information
bysort MasterPortfolioId LegalType: keep if _n == 1

* MasterPortfolioId takes LegalType=FO by default if we get aggregated reports that are repeated in the
* FO and FE universes, unless an apportioning weight is given in $temp/Morningstar_Mapping_Build/etf_weights.dta.
* We construct this apportioning weights for any fund with AUM >10 billion USD at any point in time that
* reports as a single portfolio for both their FO and ETF structures. These are primarily large Vanguard funds
* that use this idiosyncratic reporting convention.

mmerge MasterPortfolioId using "$temp/Morningstar_Mapping_Build/etf_weights.dta", unmatched(m) // We need to in the update that this file, which includes data from 1986 to 2019, shoule be upated
bysort MasterPortfolioId : egen numLeg = nvals(LegalType)
foreach i of num $firstyear/$lastyear {
	gen fundtype_weight_`i' = etf_weight_`i' if LegalType == "FE" & ~missing(etf_weight_`i')
	replace fundtype_weight_`i' = (1 - etf_weight_`i') if LegalType == "FO" & ~missing(etf_weight_`i')
	replace fundtype_weight_`i' = 1 if missing(fundtype_weight_`i') & numLeg == 1
	replace fundtype_weight_`i' = 0 if numLeg > 1 & missing(fundtype_weight_`i') & LegalType!="F0"
}
rename LegalType fundtype
drop etf_weight_*
cap drop _merge
drop numLeg
drop if fundtype_weight_$lastyear <.5
drop fundtype_weight_*
duplicates drop
save "$output/Morningstar_Mapping_Build/boothmapping_uniqueonly.dta", replace

*log close
