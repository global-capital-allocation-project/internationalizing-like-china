***************************************************************************************************
* HoldingDetail_Build_2
*
* This file reads in raw holdingdetail files, cleans and appends them, and then merges with the
* exchange rate data, integrated mapping files directly delivered by Morningstar and internal flatfile
* to get additional information like exchange rate, domicile country, status, mns_class , mns_subclass etc.
* The output are a series of clean month-year based holdingdetail files which will be improved in the later Build.
*
* Please note that from April 2020, the data files include historical observations from 2003-2020, so those
* observations are appended to each year data files.
***************************************************************************************************


***************************************************************************************************
* Setup
***************************************************************************************************

* Change argument to month and year if necessary
if `1' < 100 {
    local year = 2020 + floor(`1'/13)
    local m = `1' - floor(`1'/13)*12
}
else {
    local year = `1'
}

di "Argument 1 = `1'"
di "Year = `year'"
di "Month = `m'"

* Create local to drop unneccessary variables
local dropvarslist1 "previousportfoliodate _id country currency economicexposure couponrate-originalcouponreferencerate"
local dropvarslist2 "localcurrencycode-symbol"
local apr20varlist "MasterPortfolioId iso_country_code iso_currency_code date cusip typecode currency_id weighting numberofshare marketvalue BroadCategoryGroup DomicileCountryId mns_class mns_subclass maturitydate fundtype_mstar lcu_per_usd_eop date_m region"

* Set local to loop over for monthly files

local month_list January February March April May June July August September October November December

***************************************************************************************************
* Combine monthly and historical data for years 2019 and earlier
***************************************************************************************************

* This section combines the relevent raw data from the historical and monthly files for each year

* We time this step
timer on 1

* Append monthly new data, keeping data for relevent year, until code has looped through all available files
if `year'>= 2003 & `year' < 2020 {
    clear
    * We start in April since first monthly file is April 2020
    local i_month 4
    local i_year 2020
    local file_miss 0
    di "Code will iterate forward through monthly data until a monthly file is not found (signalling all monthly data has been appended)."
    while `file_miss'<1 {
        local current_m : word `i_month' of `month_list'
        di "Appending data from `current_m'_`i_year'..."
        capture append using "$temp/HoldingDetail/`current_m'_`i_year'.dta"
        if _rc!=0 {
            local ++file_miss
            di "File not found for `current_m'_`i_year'."
        }
        keep if year == `year'
        * Add to month indexer, change year if necessary
        local ++i_month
        if `i_month'==13 {
            local ++i_year
            local i_month = 1
        }
    }
    * Process appended data
    merge m:1 InvestmentProductId using "$output/Morningstar_Mapping_Build/boothmapping_full.dta", nogen keep(1 3)
    gen MasterPortfolioId = MasterPortfolioId_mapping
    order MasterPortfolioId, before(InvestmentProductId)
    gen fundtype = fundshareclasslegaltype
    replace fundtype = LegalType if missing(fundtype)
    replace fundshareclassname = FundName if missing(fundshareclassname)
    rename (DomicileCountryId fundtype BroadCategoryGroup region) (DomicileCountryId_mapping fundtype_mapping BroadCategoryGroup_mapping region_mapping)
    drop FundId ObsoleteType Ticker CUSIP ISIN LegalType FundName Status year ObsoleteDate FundLegalStructure MasterPortfolioId_mapping
    save "$temp/HoldingDetail/HoldingDetail_new_sub`year'.dta", replace
}

* Append historical data for each year
* Data structure changes in 2017 and 2018, so we do pre-2017, then 2017, then 2018-2019
if `year' < 2017 {
	clear
	foreach filetype in "NonUS" "US" {
		foreach fundtype in "FO" "FM" "FE" {
			foreach atype in "Active" "Inactive" {
				foreach mtype in "" "_NonMonthEnd" {
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
						cap fs "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/HoldingDetail_*.dta"
						if (_rc==0) | ( inlist(_rc,0,198,601)) {
							foreach file in `r(files)' {
								append using "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
							}
							cap drop `dropvarslist1'
							cap drop `dropvarslist2'
							cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
							gen region_mstar="Rest" if "`filetype'"=="NonUS"
							replace region_mstar="US" if "`filetype'"=="US"
							gen fundtype="`fundtype'"
							gen status_mstar="`atype'"
							cap rename _detailholdingtypeid typecode
							save "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
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
						capture append using "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
						*capture rm "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
					}
				}
			}
		}
	}
	save "$temp/HoldingDetail/HoldingDetail_`year'.dta", replace
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
						cap fs "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/HoldingDetail_*.dta"
						if (_rc==0) | ( inlist(_rc,0,198,601)) {
							foreach file in `r(files)' {
								append using "$dir_mstar_raw/historical/DTA/`year'/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
							}
							cap drop `dropvarslist1'
							cap drop `dropvarslist2'
							cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
							gen region_mstar="Rest" if "`filetype'"=="NonUS"
							replace region_mstar="US" if "`filetype'"=="US"
							gen fundtype="`fundtype'"
							gen status_mstar="`atype'"
							cap rename _detailholdingtypeid typecode
							save "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
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
						capture append using "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
						*capture rm "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
					}
				}
			}
		}
	}
	save "$temp/HoldingDetail/HoldingDetail_`year'.dta", replace
	clear
	foreach fundtype in "FO" "FM" "FE" {
		cap fs "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year17'/HoldingDetail_*.dta"
		if (_rc==0) | ( inlist(_rc,0,198,601)) {
			foreach file in `r(files)' {
				append using "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year17'/`file'"
			}
			cap drop `dropvarslist1'
			cap drop `dropvarslist2'
			cap rename (_masterportfolioid currencyid _detailholdingtypeid) (MasterPortfolioId CurrencyId typecode)
			gen fundtype = "`fundtype'"
			save "$temp/HoldingDetail/`fundtype'_`year17'.dta", replace
		}
		clear
	}
	foreach fundtype in "FO" "FM" "FE" {
		capture append using "$temp/HoldingDetail/`fundtype'_`year17'.dta"
		*capture rm "$temp/HoldingDetail/`fundtype'_`year17'.dta"
	}
	save "$temp/HoldingDetail/HoldingDetail_`year17'.dta", replace
	clear
	use "$temp/HoldingDetail/HoldingDetail_`year'.dta", clear
	capture append using "$temp/HoldingDetail/HoldingDetail_`year17'.dta"
	*capture rm "$temp/HoldingDetail/HoldingDetail_`year17'.dta"
	save "$temp/HoldingDetail/HoldingDetail_`year'.dta", replace
	clear
}
else if `year' > 2017 & `year' < 2020 {
	clear
	foreach fundtype in "FO" "FM" "FE" {
		cap fs "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/HoldingDetail_*.dta"
		if (_rc==0) | ( inlist(_rc,0,198,601)) {
			foreach file in `r(files)' {
                di "Appending `file' ..."
				append using "$dir_mstar_raw/historical/DTA/`year'/`fundtype'_`year'/`file'"
			}
			cap drop `dropvarslist1'
			cap drop `dropvarslist2'
			cap rename (_masterportfolioid _currencyid _detailholdingtypeid) (MasterPortfolioId CurrencyId typecode)
			gen fundtype = "`fundtype'"
			save "$temp/HoldingDetail/`fundtype'_`year'.dta", replace
		}
		clear
	}
	clear
	foreach fundtype in "FO" "FM" "FE" {
        di "Appending `fundtype'_`year' ..."
		capture append using "$temp/HoldingDetail/`fundtype'_`year'.dta"
		*capture rm "$temp/HoldingDetail/`fundtype'_`year'.dta"
	}
	save "$temp/HoldingDetail/HoldingDetail_`year'.dta", replace
	clear
}


***************************************************************************************************
* Finish processing HoldingDetail files for 2019 and earlier
***************************************************************************************************

/* Using yearly data to merge with API and FX data */
if `year' < 2020 {

	use "$temp/HoldingDetail/HoldingDetail_`year'.dta", clear
	rename region localregion
	gen file_m = ym(`year',1)

	capture append using "$temp/HoldingDetail/HoldingDetail_new_sub`year'.dta"
	*capture rm "$temp/HoldingDetail/HoldingDetail_new_sub`year'.dta"

	capture drop _merge
*	capture missings dropvars, force

	foreach var of varlist _all {
		capture assert missing(`var')
		if !_rc {
			drop `var'
		}
	}
	count
	if `r(N)'>0 {
		merge m:1 MasterPortfolioId using "$output/Morningstar_Mapping_Build/boothmapping_uniqueonly.dta", keep(1 3)
		capture replace fundtype = fundtype_mapping if missing(fundtype)
		if `year' <= 2017 {
            replace DomicileCountryId="USA" if region_mstar=="US" & _merge==1
		}
		capture replace DomicileCountryId = DomicileCountryId_mapping if missing(DomicileCountryId)
		gen region = "US" if DomicileCountryId == "USA"
		replace region = "Rest" if DomicileCountryId != "USA"
		if `year' <= 2017 {
            replace region="US" if region_mstar=="US"
            replace region="Rest" if region_mstar=="Rest"
		}
		capture replace region = region_mapping if missing(region)
		replace BroadCategoryGroup="Money Market" if fundtype=="FM" & _merge==1
		capture replace BroadCategoryGroup = BroadCategoryGroup_mapping if missing(BroadCategoryGroup)
		capture drop fundtype_mapping DomicileCountryId_mapping BroadCategoryGroup_mapping region_mapping
		rename fundtype fundtype_mstar

		drop if DomicileCountryId==""
		drop _merge
		capture destring _storageid, force replace
        * compress to reduce file size before merge
        compress
		merge m:1 typecode using "$dir_mstar_raw/morningstar_api_data/Categories_Asset_Class.dta", keep(1 3) nogen
		if `year' >= 2003 {
            replace date_m = mofd(date)
		}
		else if `year' < 2003 {
            gen date_m = mofd(date)
		}
		format date_m %tm
		rename CurrencyId iso_currency_code
		rename country_id iso_country_code
        * compress to reduce file size before merge
        compress
		merge m:1 iso_currency_code date_m using "$output/ER_Data/IFS_ERdata", keep(1 3) gen(merge_w_er_data) keepusing(Value)
		replace Value=1 if iso_currency_code=="USD"
		replace merge_w_er_data=3 if iso_currency_code=="USD"
		drop merge_w_er_data
		cap sort MasterPortfolioId date_m _storageid
		cap gen coupon=""
		cap gen maturitydate=.
		rename Value lcu_per_usd_eop
		gen marketvalue_usd = marketvalue/lcu_per_usd_eop
		destring weighting costbasis, replace
		foreach var in "weighting" "costbasis" {
				replace `var' = round(`var', 0.01)
		}
	    if `year' >= 2003 {
            bys MasterPortfolioId InvestmentProductId date_m: egen maxfile = max(file_m)
            bys MasterPortfolioId InvestmentProductId date_m: keep if file_m == maxfile
	    }

*	    capture missings dropvars, force
		foreach var of varlist _all {
			capture assert missing(`var')
			if !_rc {
				drop `var'
			}
		}
	    duplicates drop MasterPortfolioId date_m iso_currency_code date typecode iso_country_code cusip currency_id securityname weighting numberofshare marketvalue costbasis region, force

		*** Dealing with corrected obs after April 2020 (securityname, costbasis, file_m)
        compress
		if `year' >= 2003 {
            drop maxfile
            bys `apr20varlist': egen maxfile = max(file_m) if !missing(cusip)
            bys `apr20varlist': drop if file_m != maxfile & !missing(cusip)
	    }

		* Fixing raw import issue (region & fundtype_mstar)
		bys MasterPortfolioId iso_country_code iso_currency_code date cusip typecode currency_id securityname weighting numberofshare marketvalue costbasis  BroadCategoryGroup DomicileCountryId mns_class mns_subclass maturitydate lcu_per_usd_eop date_m file_m: gen dup_rgn_fndtyp = cond(_N==1, 0, _n)
		gen keep = 0 if dup_rgn_fndtyp != 0 & DomicileCountryId == "USA" & region != "US"
		replace keep = 0 if dup_rgn_fndtyp != 0 & DomicileCountryId != "USA" & region == "US"
		drop if keep == 0
		drop dup_rgn_fndtyp keep

	    save "$temp/HoldingDetail/HoldingDetail_`year'_m_step1", replace
	    *capture rm "$temp/HoldingDetail/HoldingDetail_`year'.dta"
	    clear
	}
}


***************************************************************************************************
* Combine monthly and historical data for years 2020 and later and process files
***************************************************************************************************

*** Generate Clean 2020 and 2021 monthly files using 2020Q1 data and raw monthly data created in HD1 do files
if `year' >= 2020 {
	clear
    * Add data from historical files if it exists (only 2020q1)
	if (`year' == 2020 & `m' < 4) {
		use "$temp/HoldingDetail/HoldingDetail_`year'_JanFebMar.dta", clear
		keep if date_m == ym(`year', `m')
	}
    * Add data from monthly files
    * We start in April since first monthly file is April 2020
    local i_month 4
    local i_year 2020
    local file_miss 0
    di "Code will iterate forward through monthly data until a monthly file is not found (signalling all monthly data has been appended)."
    while `file_miss'<1 {
        local current_m : word `i_month' of `month_list'
        di "Appending data from `current_m'_`i_year'..."
        capture append using "$temp/HoldingDetail/`current_m'_`i_year'.dta"
        if _rc!=0 {
            local ++file_miss
            di "File not found for `current_m'_`i_year'."
        }
        * Keep relevent observations
        keep if date_m == ym(`year', `m')
        * Add to month indexer, change year if necessary
        local ++i_month
        if `i_month'==13 {
            local ++i_year
            local i_month = 1
        }
    }
    * Process appended data
	cap drop `dropvarslist1'
	cap drop `dropvarslist2'
	merge m:1 InvestmentProductId using "$output/Morningstar_Mapping_Build/boothmapping_full.dta", nogen keep(1 3)
	if `year' == 2020 & `m' < 4 {
	replace MasterPortfolioId = MasterPortfolioId_mapping if missing(MasterPortfolioId)
	replace fundtype = fundshareclasslegaltype if missing(fundtype)
	}
	else if (`year' == 2020 & `m' >= 4) | `year' != 2020 {
	gen MasterPortfolioId = MasterPortfolioId_mapping
	gen fundtype = fundshareclasslegaltype
	}
	replace fundtype = LegalType if missing(fundtype)
	rename (DomicileCountryId fundtype BroadCategoryGroup region) (DomicileCountryId_mapping fundtype_mapping BroadCategoryGroup_mapping region_mapping)
	order MasterPortfolioId InvestmentProductId, after(filename)
	drop FundId ObsoleteType Ticker CUSIP ISIN LegalType FundName Status year ObsoleteDate FundLegalStructure MasterPortfolioId_mapping
	capture drop _merge
	count
	if `r(N)'>0 {
		destring weighting costbasis, replace
	    foreach var in "weighting" "costbasis" {
            replace `var' = round(`var', 0.01)
	    }

        merge m:1 MasterPortfolioId using "$output/Morningstar_Mapping_Build/boothmapping_uniqueonly.dta", keep(1 3)
        replace fundtype = fundtype_mapping if missing(fundtype)
        replace BroadCategoryGroup = "Money Market" if fundtype=="FM" & _merge==1
        replace BroadCategoryGroup = BroadCategoryGroup_mapping if missing(BroadCategoryGroup)
        replace DomicileCountryId = DomicileCountryId_mapping if missing(DomicileCountryId)
        gen region = "US" if DomicileCountryId == "USA"
        replace region = "Rest" if DomicileCountryId != "USA"
        replace region = region_mapping if missing(region)
        drop fundtype_mapping DomicileCountryId_mapping BroadCategoryGroup_mapping region_mapping
        rename fundtype fundtype_mstar
        drop if DomicileCountryId==""
        drop _merge
        capture destring _storageid, force replace

        merge m:1 typecode using "$dir_mstar_raw/morningstar_api_data/Categories_Asset_Class.dta", keep(1 3) nogen
        cap sort MasterPortfolioId date _storageid
        replace date_m = mofd(date)
        format date_m %tm
        rename CurrencyId iso_currency_code
        rename country_id iso_country_code

        * compress to reduce file size before merge
        compress
        merge m:1 iso_currency_code date_m using "$output/ER_Data/IFS_ERdata", keep(1 3) gen(merge_w_er_data) keepusing(Value)
        replace Value=1 if iso_currency_code=="USD"
        replace merge_w_er_data=3 if iso_currency_code=="USD"
        drop merge_w_er_data
        cap sort MasterPortfolioId date_m _storageid
        cap gen coupon=""
        cap gen maturitydate=.
        rename Value lcu_per_usd_eop
        gen marketvalue_usd = marketvalue/lcu_per_usd_eop

        bys MasterPortfolioId InvestmentProductId date_m: egen maxfile = max(file_m)
        bys MasterPortfolioId InvestmentProductId date_m: keep if file_m == maxfile
        duplicates drop MasterPortfolioId date_m iso_currency_code date typecode iso_country_code cusip currency_id securityname weighting numberofshare marketvalue costbasis region, force
    *	capture missings dropvars, force
        foreach var of varlist _all {
            capture assert missing(`var')
            if !_rc {
                drop `var'
            }
        }
        * These monthly files will be appended into yearly files in HoldingDetail_3
        save "$temp/HoldingDetail/HoldingDetail_`year'_m`m'.dta", replace
	}
    clear
}

timer off 1
timer list
local time_elp = r(t1)
di "Elapsed Time to generate HoldingDetail_`year' data: `time_elp'"


*log close
