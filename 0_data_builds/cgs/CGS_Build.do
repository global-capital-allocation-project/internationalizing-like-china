clear
set more off

* Read main path
global gcap_data="`2'"

cap mkdir "${gcap_data}/rmb_replication/logs"
cap log close 
log using "${gcap_data}/rmb_replication/logs/`1'.log", replace

* Install all required packages from SSC
cap ssc install mmerge
cap ssc install fs
cap ssc install unique
cap ssc install ftools
cap ssc install kountry
cap ssc install nmissing

* Install all required packages from the web
local github "https://raw.githubusercontent.com"
cap net install gtools.pkg, from(`github'/mcaceresb/stata-gtools/master/build/)

* Run Stata Code
do "${gcap_data}/rmb_replication/0_data_builds/cgs/cgs.do"

cap log close 