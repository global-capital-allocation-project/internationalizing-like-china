* --------------------------------------------------------------------------------------------------
* PortfolioSummary_Build
*
* This file reads in the PortfolioSummary files, cleans and appends them, and then merges with both 
* the API and FX data. The files, called PortfolioSummary_* (*=m,q,y) are of manageable size and can 
* be used in analyses.
* Please note that from April 2020, the data files include historical observations from 2003-2020, so those
* observations are appended to each year data files.
* --------------------------------------------------------------------------------------------------

*cap log close
*log using "$logs/PortfolioSummary/${whoami}_PortfolioSummary_Build_`1'", replace

* Append all the PortfolioSummary shards

local year = `1'

local keepvarslist "filename _masterportfolioid _currencyid date netexpenseratio totalmarketvalueshort totalmarketvaluelong"

if `year' < 2017 {
	clear
	
	foreach filetype in "NonUS" "US" {
		foreach fundtype in "FO" "FM" "FE" {
			foreach atype in "Active" "Inactive" {
				foreach mtype in "" "_NonMonthEnd" {
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
						capture fs "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/PortfolioSummary*.dta"
						if _rc==0 {
							foreach file in `r(files)' {
								append using "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
							}
							cap keep `keepvarslist'
							cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
							gen region_mstar="Rest" if "`filetype'"=="NonUS"
							replace region_mstar="US" if "`filetype'"=="US"	
							gen fundtype="`fundtype'"
							gen status_mstar="`atype'"
							save "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
						}
						clear
					}
				}
			}
		}
	}
	
	clear
	foreach filetype in "NonUS" "US" {
		foreach fundtype in "FO" "FM" "FE" {
			foreach atype in "Active" "Inactive" {
				foreach mtype in "" "_NonMonthEnd" {
					di "Appending Year `year' for `filetype'"
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {	
						capture append using "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
						capture rm "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
					}
				}
			}
		}
	}
	capture missings dropvars, force
	save "$output/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	clear

} 
	
				
else if `year' == 2017 {


	local year17 = "2017_12_31"
	clear
	
	foreach filetype in "NonUS" "US" {
		foreach fundtype in "FO" "FM" "FE" {
			foreach atype in "Active" "Inactive" {
				foreach mtype in "" "_NonMonthEnd" {
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
						capture fs "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/PortfolioSummary*.dta"
						if _rc==0 {
							foreach file in `r(files)' {
								append using "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
							}
							cap keep `keepvarslist'
							cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
							gen region_mstar="Rest" if "`filetype'"=="NonUS"
							replace region_mstar="US" if "`filetype'"=="US"	
							gen fundtype="`fundtype'"
							gen status_mstar="`atype'"
							save "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
						}
						clear
					}
				}
			}
		}
	}
	
	clear
	foreach filetype in "NonUS" "US" {
		foreach fundtype in "FO" "FM" "FE" {
			foreach atype in "Active" "Inactive" {
				foreach mtype in "" "_NonMonthEnd" {
					di "Appending Year `year' for `filetype'"
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {	
						capture append using "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
						capture rm "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
					}
				}
			}
		}
	}
	save "$temp/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	clear
	
	
	foreach fundtype in "FO" "FM" "FE" {
		capture fs "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year17'/PortfolioSummary*.dta"
		if _rc==0 {
			foreach file in `r(files)' {
				append using "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year17'/`file'"
			}
			cap keep `keepvarslist'
			cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)	
			gen fundtype = "`fundtype'"
			save "$temp/PortfolioSummary/`fundtype'_`year17'.dta", replace 
		}
		clear
	}
	
	foreach fundtype in "FO" "FM" "FE" {
		capture append using "$temp/PortfolioSummary/`fundtype'_`year17'.dta"
		capture rm "$temp/PortfolioSummary/`fundtype'_`year17'.dta"
	}
	save "$temp/PortfolioSummary/PortfolioSummary_`year17'.dta", replace
	clear
	
	use "$temp/PortfolioSummary/PortfolioSummary_`year'.dta", clear
	capture append using "$temp/PortfolioSummary/PortfolioSummary_`year17'.dta"
	capture missings dropvars, force
	save "$output/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	capture rm "$temp/PortfolioSummary/PortfolioSummary_`year17'.dta"
	capture rm "$temp/PortfolioSummary/PortfolioSummary_`year'.dta"
	clear


} 

else if `year' > 2017 & `year' < 2020 {
	
	foreach fundtype in "FO" "FM" "FE" {
		capture fs "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/PortfolioSummary*.dta"
		if _rc==0 {
			foreach file in `r(files)' {
				append using "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/`file'"
			}
			cap keep `keepvarslist'
			cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
			gen fundtype = "`fundtype'"
			save "$temp/PortfolioSummary/`fundtype'_`year'.dta", replace
		}
		clear		
	}
	
	foreach fundtype in "FO" "FM" "FE" {
		capture append using "$temp/PortfolioSummary/`fundtype'_`year'.dta"
		capture rm "$temp/PortfolioSummary/`fundtype'_`year'.dta"
	}
	capture missings dropvars, force
	save "$output/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	clear
	
} 

else if `year' >= 2020 {

	if `year' == 2020 {
	clear

	foreach fundtype in "FO" "FM" "FE" {
		capture fs "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/PortfolioSummary*.dta"
		if _rc==0 {
			foreach file in `r(files)' {
				append using "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/`file'"
			}
			cap keep `keepvarslist'
			cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)	
			gen fundtype = "`fundtype'"
			save "$temp/PortfolioSummary/`fundtype'_`year'.dta", replace 
		}
		clear
	}
	
	foreach fundtype in "FO" "FM" "FE" {
		capture append using "$temp/PortfolioSummary/`fundtype'_`year'.dta"
		capture rm "$temp/PortfolioSummary/`fundtype'_`year'.dta"
	}
	save "$temp/PortfolioSummary/PortfolioSummary_`year'_JanFebMar.dta", replace
	}
	
	clear
	
	foreach month in "January" "February" "March" "April" "May" "June" "July" "August" "September" "October" "November" "December"{
		capture fs "$dir_mstar_raw/monthly_new/DTA/`month'_`year'/PortfolioSummary*.dta"
		if _rc==0 {
			foreach file in `r(files)' {
				append using "$dir_mstar_raw/monthly_new/DTA/`month'_`year'/`file'"
			}
			local keepvarslist "filename investmentvehicleid fundshareclasslegaltype _currencyid date netexpenseratio totalmarketvalueshort totalmarketvaluelong"
			cap keep `keepvarslist'
			cap rename (investmentvehicleid _currencyid) (InvestmentProductId CurrencyId)
			save "$temp/PortfolioSummary/`month'_`year'.dta", replace
		}
		clear
	}
	
    foreach month in "January" "February" "March" "April" "May" "June" "July" "August" "September" "October" "November" "December" {	
		capture append using "$temp/PortfolioSummary/`month'_`year'.dta"
		capture rm "$temp/PortfolioSummary/`month'_`year'.dta"
    }
	merge m:1 InvestmentProductId using "$output/Morningstar_Mapping_Build/boothmapping_full.dta", nogen keep(1 3) 
	order MasterPortfolioId, before(InvestmentProductId)
	keep filename MasterPortfolioId InvestmentProductId CurrencyId date netexpenseratio totalmarketvalueshort totalmarketvaluelong
	capture missings dropvars, force
	save "$output/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	clear
	
	if `year' == 2020 {
	use "$temp/PortfolioSummary/PortfolioSummary_`year'_JanFebMar.dta", clear
	capture append using "$output/PortfolioSummary/PortfolioSummary_`year'.dta"
	capture missings dropvars, force
	save "$output/PortfolioSummary/PortfolioSummary_`year'.dta", replace
	capture rm "$temp/PortfolioSummary/PortfolioSummary_`year'_JanFebMar.dta"
	}

}

use "$output/PortfolioSummary/PortfolioSummary_`year'.dta", clear
* Merge in the fund metadata
if `year' < 2017 {		
keep *_mstar MasterPortfolioId CurrencyId date totalmarketvalue*
merge m:1 MasterPortfolioId using "$output/Morningstar_Mapping_Build/boothmapping_uniqueonly.dta", keep(1 3)
replace DomicileCountryId="USA" if region_mstar=="US" & _merge==1
}
if `year' >= 2017 {		
cap keep MasterPortfolioId CurrencyId fundtype date totalmarketvalue*
if `year' == 2021 {
keep MasterPortfolioId CurrencyId date totalmarketvalue*
rename MasterPortfolioId_mapping MasterPortfolioId
}
merge m:1 MasterPortfolioId using "$output/Morningstar_Mapping_Build/boothmapping_uniqueonly.dta", keep(1 3)
}
replace BroadCategory="Money Market" if fundtype=="FM" & _merge==1
rename fundtype fundtype_mstar
drop if DomicileCountryId==""
drop _merge
* Prepare output PortfolioSummary files; merge with exchange rates
cap drop status*
cap drop region
rename *_mstar *
sort MasterPortfolioId date
gen date_m = mofd(date)
format date_m %tm
rename CurrencyId iso_currency_code
rename DomicileCountryId iso_country_code
ds MasterPortfolioId date_m, not
collapse (lastnm) `r(varlist)', by(MasterPortfolioId date_m)
merge m:1 iso_currency date_m using "$output/ER_Data/IFS_ERdata", keep(1 3) nogen keepusing(Value)
replace Value = 1 if iso_currency_code == "USD"
save $output/PortfolioSummary/PortfolioSummary_`year'_m, replace
gen month = month(date)
keep if month==3 | month==6 | month==9 | month==12
gen date_q = qofd(date)
format date_q %tq
drop month
sort date_q MasterPortfolioId
save $output/PortfolioSummary/PortfolioSummary_`year'_q, replace
gen month = month(date)
keep if month == 12
gen date_y = yofd(date)
format date_y %ty
drop month
sort date_y MasterPortfolioId
save $output/PortfolioSummary/PortfolioSummary_`year'_y, replace
clear

capture fs "$output/PortfolioSummary/PortfolioSummary_*.dta"
local count = 0
if _rc==0 {
  foreach file in `r(files)' {
  local count = `count' + 1
}
}

global lastyear=$lastyear+1
if `count' == ($lastyear-$firstyear+1)*4 {
foreach type in "" "_m" "_q" "_y"{
    clear
	forvalues yr = $firstyear/$lastyear {
		capture append using "$output/PortfolioSummary/PortfolioSummary_`yr'`type'"
	}
	cap drop year
	gen year = year(date)
	preserve
	forvalues yr = $firstyear/$lastyear {
		restore
		preserve
		keep if year == `yr'
		save "$output/PortfolioSummary/PortfolioSummary_`yr'`type'", replace
		clear
	}
	restore
	clear
}
}

*log close
