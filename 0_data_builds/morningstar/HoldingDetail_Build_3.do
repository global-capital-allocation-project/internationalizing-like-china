* ------------------------------------------------------------------------------------------------------------------
* HoldingDetail_Build_3
*
* This file appends the monthly files for years 2020 on to create annual files.
* ------------------------------------------------------------------------------------------------------------------

*cap log close
*log using "$logs/HoldingDetail/${whoami}_HoldingDetail_Build_3_Array`1'.log" , replace

* change argument to month and year if necessary
if `1' < 100 {
    local year = 2020 + floor(`1'/13)
    local m = `arg1' - floor(`1'/13)*12
}
else {
    local year = `1'
}

di "Argument 1 = `1'"
di "Year = `year'"

clear

fs "$temp/HoldingDetail/HoldingDetail_`year'_m*.dta"
local count = 0
foreach file in `r(files)' {
    local count = `count' + 1
}

forvalues m = 1/`count' {
	capture append using "$temp/HoldingDetail/HoldingDetail_`year'_m`m'.dta"
	*capture rm "$temp/HoldingDetail/HoldingDetail_`year'_m`m'.dta"
}
*	capture missings dropvars, force
foreach var of varlist _all {
	capture assert missing(`var')
	if !_rc {
		drop `var'
	}
}
save "$temp/HoldingDetail/HoldingDetail_`year'_m_step1", replace



*log close
