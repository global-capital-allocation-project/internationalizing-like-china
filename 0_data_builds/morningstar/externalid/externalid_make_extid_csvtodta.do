* --------------------------------------------------------------------------------------------------
* make_extid_csvtodta
*
* This job converts the internally-generated externalid master flatfile from CSV to Stata format.
* --------------------------------------------------------------------------------------------------

* Transforms externalid master file from csv to dta.
clear all

insheet using "$gcap_data/output/morningstar/temp/externalid/extid_master.csv", names

ds,has (type string)
foreach var in `r(varlist)' {
    replace `var' = "" if `var' == "NA"
}
* Fix maturitydate format
gen maturitydate_cln = date(maturitydate, "20YMD")
format maturitydate_cln %d
drop maturitydate
rename maturitydate_cln maturitydate
recast long maturitydate
order externalid_mns-securityname maturitydate coupon-nunique_portid
save "$gcap_data/output/morningstar/temp/externalid/extid_master.dta", replace emptyok

*log close
