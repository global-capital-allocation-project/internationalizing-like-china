* --------------------------------------------------------------------------------------------------
* HoldingDetail_Build_1
*
* This file generates raw monthly data files from 2020 which will be used in HoldingDetail_Build_2
* to create a clean version of HoldingDetail files.
* --------------------------------------------------------------------------------------------------

*cap log close
*log using "$logs/HoldingDetail/${whoami}_HoldingDetail_Build_1_Array_`1'.log", replace

local dropvarslist1 "previousportfoliodate _id country currency economicexposure couponrate-originalcouponreferencerate"
local dropvarslist2 "localcurrencycode-symbol"

**** Generate raw monthly files for data with new format (April 2020 -)

* parse arguments into year and month
local year = 2020 + floor(`1'/13)
local m = `1' - floor(`1'/13)*12

local month January February March April May June July August September October November December
local current_m : word `m' of `month'

di "Year: `year'"
di "Month: `current_m'"

* for March 2020, make HoldingDetail_2020_JanFebMar file that will be used by Jan, Feb, Mar 2020 
if (`year' == 2020 & `m' == 3) {
    foreach fundtype in "FO" "FM" "FE" {
        cap fs "$dir_mstar_raw/historical/DTA/2020/`fundtype'_2020/HoldingDetail_*.dta"
        di _rc
        if ((_rc==0) | (inlist(_rc,0,198,601))) {
            foreach file in `r(files)' {
                di "Appending `file' ..."
                append using "$dir_mstar_raw/historical/DTA/2020/`fundtype'_2020/`file'"
            }
            cap drop `dropvarslist1'
            cap drop `dropvarslist2'
            cap rename (_masterportfolioid _currencyid _detailholdingtypeid) (MasterPortfolioId CurrencyId typecode)
            gen fundtype = "`fundtype'"
            save "$temp/HoldingDetail/`fundtype'_2020.dta", replace
        }
        clear
    }
    clear
    foreach fundtype in "FO" "FM" "FE" {
        capture append using "$temp/HoldingDetail/`fundtype'_2020.dta"
        * capture rm "$temp/HoldingDetail/`fundtype'_2020.dta"
    }
    gen date_m = mofd(date)
    format date_m %tm
    gen file_m = ym(2020, 3)
    drop region
    save "$temp/HoldingDetail/HoldingDetail_2020_JanFebMar.dta", replace
}

* for other month years save version of monthly data
else {
    clear all
    cap fs "$dir_mstar_raw/monthly_new/DTA/`current_m'_`year'/HoldingDetail_*.dta"
    di _rc
    * the fs command sometimes incorrectly returns error codes on sherlock
    if (_rc==0) | (inlist(_rc,0,198,601)) {
        foreach file in `r(files)' {
            di "Appending `file' ..."
            append using "$dir_mstar_raw/monthly_new/DTA/`current_m'_`year'/`file'"
        }
        cap drop `dropvarslist1'
        cap drop `dropvarslist2'
        drop _masterportfolioid
        cap rename (investmentvehicleid _currencyid legaltype region) (InvestmentProductId CurrencyId typecode localregion)
        gen date_m = mofd(date)
        format date_m %tm
        gen year = year(date)
        gen file_m = ym(`year',`m')
        save "$temp/HoldingDetail/`current_m'_`year'.dta", replace
        di "Saved $temp/HoldingDetail/`current_m'_`year'.dta"
    }
}

*cap log close
