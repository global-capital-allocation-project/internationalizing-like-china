* --------------------------------------------------------------------------------------------------
* Manual_Corrections
* 
* Drop obs with abnormal positions
* --------------------------------------------------------------------------------------------------


if inrange(2011,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2011_m_cusipmerge.dta", clear
    *** Dropping Indian funds with huge commercial paper (class=C, subclass=0) positions in 2011m9
    drop if (MasterPortfolioId==145676 | MasterPortfolioId==459778) & date_m==tm(2011m9) & mns_class=="C" & mns_subclass=="O"
    save "$temp/HoldingDetail/HoldingDetail_2011_m_cusipmerge.dta", replace
}

if inrange(2010,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2010_m_cusipmerge.dta", clear
    *** Dropping GBR fund with huge corporate positions (class=B, subclass=C) in 2010m9, look like mortgage holdings
    drop if MasterPortfolioId==583448 & date_m==tm(2010m9) & mns_class=="B" & mns_subclass=="C"
    *** Dropping CHE fund with huge single DEU gov't position (class=B, subclass=S) in 2010m9
    drop if MasterPortfolioId==58384 & date_m==tm(2010m9) & cusip=="D20658BD7" & marketvalue>1e12
    save "$temp/HoldingDetail/HoldingDetail_2010_m_cusipmerge.dta", replace
}

if inrange(2008,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2008_m_cusipmerge.dta", clear
    *** Dropping USA fund with small number of huge sovereign positions in Asian Countries (class=B, subclass=S) in 2008m6
    *** Looks very much like positions reported in LCU vs. USD, though USD is what is recorded
    drop if MasterPortfolioId==303290 & date_m==tm(2008m6)
    save "$temp/HoldingDetail/HoldingDetail_2008_m_cusipmerge.dta", replace
}

if inrange(2004,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2004_m_cusipmerge.dta", clear
    *** Dropping non-equity positions of 5 Italian funds during 2004m3, which were super high
    drop if MasterPortfolioId==49798 & date_m==tm(2004m3) & mns_class!="E"
    drop if MasterPortfolioId==49793 & date_m==tm(2004m3) & mns_class!="E"
    drop if MasterPortfolioId==49791 & date_m==tm(2004m3) & mns_class!="E"
    drop if MasterPortfolioId==49792 & date_m==tm(2004m3) & mns_class!="E"
    drop if MasterPortfolioId==49797 & date_m==tm(2004m3) & mns_class!="E"
    save "$temp/HoldingDetail/HoldingDetail_2004_m_cusipmerge.dta", replace
}

if inrange(2010,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2010_m_cusipmerge.dta", clear
    *** Dropping non-equity positions of 9 Spanish funds during 2010m6, which were super high
    drop if MasterPortfolioId==45163 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==45168 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==45173 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==46734 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==54066 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==54486 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==56778 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==188511 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==328538 & date_m==tm(2010m6) & mns_class!="E"
    drop if MasterPortfolioId==373768 & date_m==tm(2010m6) & mns_class!="E"
    save "$temp/HoldingDetail/HoldingDetail_2010_m_cusipmerge.dta", replace
}

if inrange(2013,$firstyear,$lastyear) {
    use "$temp/HoldingDetail/HoldingDetail_2013_m_cusipmerge.dta", clear
    *** Dropping giant cash position of Brazilian Equity fund in 2013q2
    drop if MasterPortfolioId==665699 & date_m==tm(2013m6) & mns_class=="C"
    save "$temp/HoldingDetail/HoldingDetail_2013_m_cusipmerge.dta", replace
}

