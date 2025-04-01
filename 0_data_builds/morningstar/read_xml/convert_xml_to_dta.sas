*-------------------------------------------------------------------------------------------------*;
* Extract_Morningstar_XLM_Data                                                                    *;
*                                                                                                 *;
* This SAS script handles extraction of the raw Morningstar XML files                             *;
*-------------------------------------------------------------------------------------------------*;
options nocenter mprint mlogic fullstimer nosyntaxcheck;
proc options;
run;

proc options option = work value define;
run;
proc datasets library = work kill;
run;
quit;

**********************************************************************;
* SYSPARM has contents of SYSPARM option on SAS command line         *;
* Remember to put a comma between the three passed parameters        *;
* The infile path is the path to the directory that holds INFILENAME *;
**********************************************************************;
%let sparm = &sysparm.;
data work.temp;
  sparm = "&sparm.";
  xallsparm = symget('sysparm');
  put sparm;
run;
%macro path(x)/parmbuff;
      %global input output file filename;
      %let input = %scan(&syspbuff,1," ,()");
      %let output = %scan(&syspbuff,2," ,()");
      %let file = %scan(&syspbuff,3," ,()");
      %let filename = %scan(&syspbuff,4," ,()");
%put  Input Path: &input.;
%put Output Path: &output.;
%put  Input File: &file.;
%put File Name: &filename.;
%mend;
%path (&sparm.);
data _null_;
run;

*****************************************************************************************;
* Read XML file, save to INTEXT (not really needed, it's just to validate things)       *;
*                        HoldingDetailsX, and PortfolioSummaryX                         *;
* We read the file as an unformatted file (recfm = n), read in each field and determine *;
* what variable to write it to.  That's what all the IF statements are doing            *;
*****************************************************************************************;
data  work.PortfolioSummary (keep = Filename
                                    _MasterPortfolioId
                                    InvestmentVehicleId
                                    FundShareClassId
                                    FundShareClassName
                                    FundShareClassLegalType
                                    Date
                                    _CurrencyId
                                    Portfolio_ExternalId
                                    PreviousPortfolioDate
                                    NetExpenseRatio
                                    NumberOfHoldingShort
                                    NumberOfStockHoldingShort
                                    NumberOfBondHoldingShort
                                    TotalMarketValueShort
                                    NumberOfHoldingLong
                                    NumberOfStockHoldingLong
                                    NumberOfBondHoldingLong
                                    TotalMarketValueLong
                                    BreakdownValueShort_Type1
                                    BreakdownValueShort_Type2
                                    BreakdownValueShort_Type3
                                    BreakdownValueShort_Type4
                                    BreakdownValueShort_Type5
                                    BreakdownValueShort_Type6
                                    BreakdownValueShort_Type7
                                    BreakdownValueShort_Type8
                                    BreakdownValueLong_Type1
                                    BreakdownValueLong_Type2
                                    BreakdownValueLong_Type3
                                    BreakdownValueLong_Type4
                                    BreakdownValueLong_Type5
                                    BreakdownValueLong_Type6
                                    BreakdownValueLong_Type7
                                    BreakdownValueLong_Type8
                                    BreakdownValueNeither_Type1
                                    BreakdownValueNeither_Type2
                                    BreakdownValueNeither_Type3
                                    BreakdownValueNeither_Type4
                                    BreakdownValueNeither_Type5
                                    BreakdownValueNeither_Type6
                                    BreakdownValueNeither_Type7
                                    BreakdownValueNeither_Type8)
                                    
     work.HoldingDetail (keep =    Filename
                                    _MasterPortfolioId
                                    InvestmentVehicleId
                                    FundShareClassId
                                    FundShareClassName
                                    FundShareClassLegalType
                                    Date
                                    _CurrencyId
                                    Portfolio_ExternalId
                                    PreviousPortfolioDate
                                    NetExpenseRatio
                                    _DetailHoldingTypeId
                                    _StorageId
                                    _ExternalId
                                    _Id
                                    ExternalName
                                    Country_Id
                                    Country
                                    CUSIP
                                    SEDOL
                                    ISIN
                                    Ticker
                                    Currency
                                    Currency_Id
                                    SecurityName
                                    LocalSecurityType
                                    LegalType
                                    LocalName
                                    Weighting
                                    NumberOfShare
                                    OriginalNumberOfShare
                                    MarketValue
                                    OriginalMarketValue
                                    EconomicExposure
                                    AccountingValue
                                    AccountingLocalValue
                                    DealId
                                    CostBasis
                                    ShareChange
                                    Sector
                                    HoldingYTDReturn
                                    MaturityDate
                                    AccruedInterest
                                    StrikePrice
                                    Coupon
                                    CouponRate
                                    CouponDayCount
                                    CouponSpread 
                                    CouponPaymentFreqency
                                    CouponReferenceRate 
                                    OriginalCouponReferenceRate
                                    Region
                                    Duration
                                    IndustryId
                                    GlobalIndustryId
                                    GlobalSector
                                    GICSIndustryId
                                    LocalCurrencyCode
                                    LocalMarketValue
                                    ZAFAssetType
                                    PaymentType
                                    Rule144AEligible
                                    AltMinTaxEligible
                                    BloombergTicker
                                    ISOExchangeID
                                    ContractSize
                                    ContractEffectiveDate
                                    SecondarySectorId
                                    CompanyId
                                    CompanyName 
                                    FirstBoughtDate
                                    MexicanTipoValor
                                    MexicanSerie
                                    MexicanEmisora
                                    UnderlyingSecId
                                    UnderlyingSecurityName
                                    PerformanceId
                                    UnderlyingPerformanceId
                                    LessThanOneYearBond
                                    LessThan92DaysBond
                                    ZAFBondIssuerClass
                                    IndianCreditQualityClassificatio
                                    IndianIndustryClassification
                                    ChileanIssuerRut 
                                    ChileanNemotecnico 
                                    ChileanInstrumentType 
                                    RTSymbol 
                                    RTRoot 
                                    RTExchangeId 
                                    RTSecurityType
                                    OSICode
                                    DerivativeTenor
                                    StyleBox
                                    Symbol);                                   
 length XMLTag                              $128
        intext                              $1024
        hdtext_01-hdtext_20                 $80
        filnam                              $128
        FileName                            $128
        Section                             $6
        _MasterPortfolioId                  8
        InvestmentVehicleId                 $32
        FundShareClassId                    $32
        FundShareClassName                  $128                              
        FundShareClassLegalType             $16
        _CurrencyId                         $32
        Portfolio_ExternalId                $32
        Date                                8
        PreviousPortfolioDate               8
        NetExpenseRatio                     $32
        NumberOfHoldingShort                8
        NumberOfStockHoldingShort           8
        NumberOfBondHoldingShort            8
        TotalMarketValueShort               8
        NumberOfHoldingLong                 8
        NumberOfStockHoldingLong            8
        NumberOfBondHoldingLong             8
        TotalMarketValueLong                8
        BreakdownValueShort_Type1           8
        BreakdownValueShort_Type2           8
        BreakdownValueShort_Type3           8
        BreakdownValueShort_Type4           8
        BreakdownValueShort_Type5           8
        BreakdownValueShort_Type6           8
        BreakdownValueShort_Type7           8
        BreakdownValueShort_Type8           8
        BreakdownValueLong_Type1            8
        BreakdownValueLong_Type2            8
        BreakdownValueLong_Type3            8
        BreakdownValueLong_Type4            8
        BreakdownValueLong_Type5            8
        BreakdownValueLong_Type6            8
        BreakdownValueLong_Type7            8
        BreakdownValueLong_Type8            8
        BreakdownValueNeither_Type1         8
        BreakdownValueNeither_Type2         8
        BreakdownValueNeither_Type3         8
        BreakdownValueNeither_Type4         8
        BreakdownValueNeither_Type5         8
        BreakdownValueNeither_Type6         8
        BreakdownValueNeither_Type7         8
        BreakdownValueNeither_Type8         8
        _Id                                 $32
        _DetailHoldingTypeId                $32
        _StorageId                          $16
        _ExternalId                         $32
        ExternalName                        $128
        Country                             $32
        Country_Id
        CUSIP                               $32
        SEDOL                               $32
        ISIN                                $12
        Ticker                              $32
        Currency                            $32
        Currency_Id                         $32
        SecurityName                        $128
        LocalSecurityType                   $32
        LegalType                           $6
        LocalName                           $32
        Weighting                           $32
        NumberOfShare                       8
        OriginalNumberOfShare               8
        SharePercentage                     $32
        NumberOfJointlyOwnedShare           8
        MarketValue                         8
        OriginalMarketValue                 8
        EconomicExposure                    8
        AccountingValue                     8 
        AccountingLocalValue                8
        DealId                              $32
        CostBasis                           $32
        ShareChange                         8
        Sector                              $32
        HoldingYTDReturn                    8
        MaturityDate                        8
        AccruedInterest                     $32
        StrikePrice                         $32
        Coupon                              $32
        CouponRate                          $32
        CouponDayCount                      $32
        CouponSpread                        $32 
        CouponPaymentFreqency               $32
        CouponReferenceRate                 $32
        OriginalCouponReferenceRate         $32
        Region                              $6
        CreditQuality                       $32
        Duration                            $32
        IndustryId                          8
        GlobalIndustryId                    8
        GlobalSector                        8
        GICSIndustryId                      $32
        LocalCurrencyCode                   $3
        LocalMarketValue                    8
        ZAFAssetType                        $3
        PaymentType                         $32
        Rule144AEligible                    $32
        AltMinTaxEligible                   $32
        BloombergTicker                     $32
        ISOExchangeID                       $32
        ContractSize                        $32
        ContractEffectiveDate               8
        SecondarySectorId                   8
        CompanyId                           $32
        CompanyName                         $128
        FirstBoughtDate                     8
        MexicanTipoValor                    $32
        MexicanSerie                        $32
        MexicanEmisora                      $32
        UnderlyingSecId                     $32
        UnderlyingSecurityName              $32
        PerformanceId                       $32
        UnderlyingPerformanceId             $32
        LessThanOneYearBond                 $32
        LessThan92DaysBond                  $32
        ZAFBondIssuerClass                  $4
        IndianCreditQualityClassificatio    $25   /*IndianCreditQualityClassification*/
        IndianIndustryClassification        $32
        ChileanIssuerRut                    $32
        ChileanNemotecnico                  $32
        ChileanInstrumentType               $6
        RTSymbol                            $32 
        RTRoot                              $32
        RTExchangeId                        $10 
        RTSecurityType                      $6
        SurveyedWeighting                   $32
        OSICode                             $32
        DerivativeTenor                     $16
        StyleBox                            8
        Symbol                              $6;
 informat _MasterPortfolioId                  comma24.0
          InvestmentVehicleId                 $32.
          FundShareClassId                    $32.
          FundShareClassName                  $128.                              
          FundShareClassLegalType             $16.
          _CurrencyId                         $32.
          Portfolio_ExternalId                $32.
          Date                                IS8601DA10.
          PreviousPortfolioDate               IS8601DA10.
          NetExpenseRatio                     $32.
          NumberOfHoldingShort                comma24.0
          NumberOfStockHoldingShort           comma24.0
          NumberOfBondHoldingShort            comma24.0
          TotalMarketValueShort               comma24.0
          NumberOfHoldingLong                 comma24.0
          NumberOfStockHoldingLong            comma24.0
          NumberOfBondHoldingLong             comma24.0
          TotalMarketValueLong                comma24.0
          BreakdownValueShort_Type1           comma24.0
          BreakdownValueShort_Type2           comma24.0
          BreakdownValueShort_Type3           comma24.0
          BreakdownValueShort_Type4           comma24.0
          BreakdownValueShort_Type5           comma24.0
          BreakdownValueShort_Type6           comma24.0
          BreakdownValueShort_Type7           comma24.0
          BreakdownValueShort_Type8           comma24.0
          BreakdownValueLong_Type1            comma24.0
          BreakdownValueLong_Type2            comma24.0
          BreakdownValueLong_Type3            comma24.0
          BreakdownValueLong_Type4            comma24.0
          BreakdownValueLong_Type5            comma24.0
          BreakdownValueLong_Type6            comma24.0
          BreakdownValueLong_Type7            comma24.0
          BreakdownValueLong_Type8            comma24.0
          BreakdownValueNeither_Type1         comma24.0
          BreakdownValueNeither_Type2         comma24.0
          BreakdownValueNeither_Type3         comma24.0
          BreakdownValueNeither_Type4         comma24.0
          BreakdownValueNeither_Type5         comma24.0
          BreakdownValueNeither_Type6         comma24.0
          BreakdownValueNeither_Type7         comma24.0
          BreakdownValueNeither_Type8         comma24.0 
          _Id                                 $32.
          _DetailHoldingTypeId                $32.
          _StorageId                          $26.
          _ExternalId                         $32.
          ExternalName                        $128.
          Country                             $32.
          Country_Id                          $32.
          CUSIP                               $32.
          SEDOL                               $32.
          ISIN                                $12.
          Ticker                              $32.
          Currency                            $32.
          Currency_Id                         $32.
          SecurityName                        $128.
          LocalSecurityType                   $32.
          LegalType                           $6.
          LocalName                           $32.
          Weighting                           $32.
          NumberOfShare                       comma24.0
          OriginalNumberOfShare               comma24.0
          SharePercentage                     $32.
          NumberOfJointlyOwnedShare           comma24.0
          MarketValue                         comma24.0
          OriginalMarketValue                 comma24.0
          EconomicExposure                    comma24.0
          AccountingValue                     comma24.0
          AccountingLocalValue                comma24.0
          DealId                              $32.
          CostBasis                           $32.
          ShareChange                         comma24.0
          Sector                              $32.
          HoldingYTDReturn                    comma24.0         
          MaturityDate                        IS8601DA10.
          AccruedInterest                     $32.
          StrikePrice                         $32.
          Coupon                              $32.
          CouponRate                          $32.
          CouponDayCount                      $32.
          CouponSpread                        $32. 
          CouponPaymentFreqency               $32.
          CouponReferenceRate                 $32.
          OriginalCouponReferenceRate         $32.
          Region                              $6.
          CreditQuality                       $32.
          Duration                            $32.
          IndustryId                          comma24.0
          GlobalIndustryId                    comma24.0
          GlobalSector                        comma24.0
          GICSIndustryId                      $32.
          LocalCurrencyCode                   $3.
          LocalMarketValue                    comma24.0
          ZAFAssetType                        $3.
          PaymentType                         $32.
          Rule144AEligible                    $32.
          AltMinTaxEligible                   $32.
          BloombergTicker                     $32.
          ISOExchangeID                       $32.
          ContractSize                        $32.
          ContractEffectiveDate               IS8601DA10.
          SecondarySectorId                   comma24.0
          CompanyId                           $32.
          CompanyName                         $128.
          FirstBoughtDate                     IS8601DA10.
          MexicanTipoValor                    $32.
          MexicanSerie                        $32.
          MexicanEmisora                      $32.
          UnderlyingSecId                     $32.
          UnderlyingSecurityName              $32.
          PerformanceId                       $32.
          UnderlyingPerformanceId             $32.
          LessThanOneYearBond                 $32.
          LessThan92DaysBond                  $32.
          ZAFBondIssuerClass                  $4.
          IndianCreditQualityClassificatio    $25.  /*IndianCreditQualityClassification*/
          IndianIndustryClassification        $32.
          ChileanIssuerRut                    $32.
          ChileanNemotecnico                  $32.
          ChileanInstrumentType               $6. 
          RTSymbol                            $32. 
          RTRoot                              $32.
          RTExchangeId                        $10. 
          RTSecurityType                      $6.
          SurveyedWeighting                   $32.
          OSICode                             $32.
          DerivativeTenor                     $16.
          Stylebox                            comma24.0
          Symbol                              $6.;
retain  hdtext_01-hdtext_20
        filnam
        FileName
        Section
        _MasterPortfolioId
        InvestmentVehicleId
        FundShareClassId
        FundShareClassName
        FundShareClassLegalType
        _CurrencyId
        Portfolio_ExternalId
        Date
        PreviousPortfolioDate
        NetExpenseRatio
        NumberOfHoldingShort
        NumberOfStockHoldingShort
        NumberOfBondHoldingShort
        TotalMarketValueShort
        NumberOfHoldingLong
        NumberOfStockHoldingLong
        NumberOfBondHoldingLong
        TotalMarketValueLong
        BreakdownValueShort_Type1
        BreakdownValueShort_Type2
        BreakdownValueShort_Type3
        BreakdownValueShort_Type4
        BreakdownValueShort_Type5
        BreakdownValueShort_Type6
        BreakdownValueShort_Type7
        BreakdownValueShort_Type8
        BreakdownValueLong_Type1
        BreakdownValueLong_Type2
        BreakdownValueLong_Type3
        BreakdownValueLong_Type4
        BreakdownValueLong_Type5
        BreakdownValueLong_Type6
        BreakdownValueLong_Type7
        BreakdownValueLong_Type8
        BreakdownValueNeither_Type1
        BreakdownValueNeither_Type2
        BreakdownValueNeither_Type3
        BreakdownValueNeither_Type4
        BreakdownValueNeither_Type5
        BreakdownValueNeither_Type6
        BreakdownValueNeither_Type7
        BreakdownValueNeither_Type8
        _Id
        _DetailHoldingTypeId
        _StorageId
        _ExternalId
        ExternalName
        Country
        Country_Id
        CUSIP
        SEDOL
        ISIN
        Ticker
        Currency
        Currency_Id
        SecurityName
        LocalSecurityType
        LegalType
        LocalName
        Weighting
        NumberOfShare
        OriginalNumberOfShare
        SharePercentage
        NumberOfJointlyOwnedShare
        MarketValue
        OriginalMarketValue
        EconomicExposure
        AccountingValue
        AccountingLocalValue
        DealId
        CostBasis
        ShareChange
        Sector
        HoldingYTDReturn
        MaturityDate
        AccruedInterest
        StrikePrice
        Coupon
        CouponRate
        CouponDayCount
        CouponSpread 
        CouponPaymentFreqency
        CouponReferenceRate 
        OriginalCouponReferenceRate
        Region
        CreditQuality
        Duration
        IndustryId
        GlobalIndustryId
        GlobalSector
        GICSIndustryId
        LocalCurrencyCode
        LocalMarketValue
        ZAFAssetType
        PaymentType
        Rule144AEligible
        AltMinTaxEligible
        BloombergTicker
        ISOExchangeID
        ContractSize
        ContractEffectiveDate
        SecondarySectorId
        CompanyId
        CompanyName 
        FirstBoughtDate
        MexicanTipoValor
        MexicanSerie
        MexicanEmisora
        UnderlyingSecId
        UnderlyingSecurityName
        PerformanceId
        UnderlyingPerformanceId
        LessThanOneYearBond
        LessThan92DaysBond
        ZAFBondIssuerClass
        IndianCreditQualityClassificatio
        IndianIndustryClassification
        ChileanIssuerRut 
        ChileanNemotecnico 
        ChileanInstrumentType
        RTSymbol 
        RTRoot 
        RTExchangeId 
        RTSecurityType
        SurveyedWeighting
        OSICode
        DerivativeTenor
        StyleBox
        Symbol;
array nray _MasterPortfolioId-numeric-Symbol;
array cray  Section-character-Symbol;
array hrayc _Id-character-Symbol;
array hrayn _Id-numeric-Symbol;
array hdtext hdtext_01-hdtext_20;
infile "&input.&file."
       recfm = N filename = filnam dlm = "<";
       filename = filnam;
       input intext;
       intext = left(trim(intext));
       XMLTag = lowcase(scan(intext, 1, "<"));
       XMLTag1 = scan(XMLTag,1,"<> ");
       if XMLTag =: '/holdingdetail>' then do;
   output work.HoldingDetail;
   do i = 1 to 20;
      hdtext{i} = "";
   end;
   return;
end;

if XMLTag =: '?xml version="1.0" encoding="utf-8"?>' then do;
  do over nray;
     nray = .;
  end;
  do over cray;
     cray = "";
  end;
end;
if XMLTag =: 'package>' then do;
  do over nray;
     nray = .;
  end;
  do over cray;
     cray = "";
  end;
end;
if XMLTag =: "portfolio _masterportfolioid" then do;
     _MasterPortfolioId = scan(intext, 3,'<>= "');
     _CurrencyId        = scan(intext,-1,'<>= "');
end;
else if XMLTag =: "investmentvehicle _id" then do;
     InvestmentVehicleId = scan(intext, 3,'<>= "');
end;
else if XMLTag =: "fundshareclass _id" then do;
     FundShareClassId = scan(intext, 3,'<>= "');
end;
else if XMLTag =: "name>" then do;
     FundShareClassName = scan(intext,-1,'<>');
end;
else if XMLTag =: "legaltype _id" then do;
     FundShareClassLegalType = scan(intext, 3,'<>= "');
end;
else if XMLTag =: "portfolio _currencyid" then do;
     _CurrencyId  = scan(intext,3,'<>= "');
     Portfolio_ExternalId  = scan(intext,5,'<>= "');
end;
else if XMLTag =: "/portfoliosummary>" then do;
     output work.PortfolioSummary;
end;
else if XMLTag =: "date>"                               then Date = input(scan(intext,-1,'<>'),yymmdd10.);
else if XMLTag =: "previousportfoliodate>"              then PreviousPortfolioDate = input(scan(intext,-1,'<>'),yymmdd10.);
else if XMLTag =: "netexpenseratio>"                    then NetExpenseRatio = input(scan(intext,-1,'<>'),comma16.0);
else if XMLTag =: 'holdingaggregate _saleposition="s">' then do;
          NumberOfHoldingShort        = .;
          NumberOfStockHoldingShort   = .;
          NumberOfBondHoldingShort    = .;
          TotalMarketValueShort       = .;
          Section = "S";
end;
else if XMLTag =: 'holdingaggregate _saleposition="l">' then do;
          NumberOfHoldingLong         = .;
          NumberOfStockHoldingLong    = .;
          NumberOfBondHoldingLong     = .;
          TotalMarketValueLong        = .;
          Section = "L";
end;
else if XMLTag =: 'portfoliobreakdown _saleposition="s">' then do;
          BreakdownValueShort_Type1 = .;
          BreakdownValueShort_Type2 = .;
          BreakdownValueShort_Type3 = .;
          BreakdownValueShort_Type4 = .;
          BreakdownValueShort_Type5 = .;
          BreakdownValueShort_Type6 = .;
          BreakdownValueShort_Type7 = .;
          BreakdownValueShort_Type8 = .;
          Section = "BreakS";
end;
else if XMLTag =: 'portfoliobreakdown _saleposition="l">' then do;
          BreakdownValueLong_Type1 = .;
          BreakdownValueLong_Type2 = .;
          BreakdownValueLong_Type3 = .;
          BreakdownValueLong_Type4 = .;
          BreakdownValueLong_Type5 = .;
          BreakdownValueLong_Type6 = .;
          BreakdownValueLong_Type7 = .;
          BreakdownValueLong_Type8 = .;
          Section = "BreakL";
end;
else if XMLTag =: 'portfoliobreakdown _saleposition="n">' then do;
          BreakdownValueNeither_Type1 = .;
          BreakdownValueNeither_Type2 = .;
          BreakdownValueNeither_Type3 = .;
          BreakdownValueNeither_Type4 = .;
          BreakdownValueNeither_Type5 = .;
          BreakdownValueNeither_Type6 = .;
          BreakdownValueNeither_Type7 = .;
          BreakdownValueNeither_Type8 = .;
          Section = "BreakN";
end;

else if XMLTag =: 'holdingdetail' then do;
          do k = 1 to 20;
            hdtext{k} = left(scan(intext,k,'<>="'));
          end;
          hdtext{1} = left(scan(hdtext{1},2,' <>="'));
          do over hrayc;
              hrayc = "";
          end;
          do over hrayn;
             hrayn = .;
          end;
          Section = "D";
          do j = 1 to 19 by 2;
                   if lowcase(hdtext{j}) =: "_detailholdingtypeid" then _DetailHoldingTypeId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "externalname"         then         ExternalName = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_storageid"           then           _StorageId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_externalid"          then          _ExternalId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_id"                  then                  _Id = hdtext{j+1};
          end;
end;
if Section = "S" then do;
        if XMLTag =: 'numberofholding'      then NumberOfHoldingShort      = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofstockholding' then NumberOfStockHoldingShort = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofbondholding'  then NumberOfBondHoldingShort  = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'totalmarketvalue'     then TotalMarketValueShort     = input(scan(intext,-1,'<>'),comma16.0);
end;
if Section = "L" then do;
        if XMLTag =: 'numberofholding'      then NumberOfHoldingLong       = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofstockholding' then NumberOfStockHoldingLong  = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofbondholding'  then NumberOfBondHoldingLong   = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'totalmarketvalue'     then TotalMarketValueLong      = input(scan(intext,-1,'<>'),comma16.0);
end;

if Section = "BreakS" then do;
          if XMLTag =: 'breakdownvalue type="1">'  then BreakdownValueShort_Type1 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="2">'  then BreakdownValueShort_Type2 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="3">'  then BreakdownValueShort_Type3 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="4">'  then BreakdownValueShort_Type4 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="5">'  then BreakdownValueShort_Type5 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="6">'  then BreakdownValueShort_Type6 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="7">'  then BreakdownValueShort_Type7 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="8">'  then BreakdownValueShort_Type8 = input(scan(intext, 4,'<>= "'),comma24.0);
end;
if Section = "BreakL" then do;
          if XMLTag =: 'breakdownvalue type="1">'  then BreakdownValueLong_Type1 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="2">'  then BreakdownValueLong_Type2 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="3">'  then BreakdownValueLong_Type3 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="4">'  then BreakdownValueLong_Type4 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="5">'  then BreakdownValueLong_Type5 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="6">'  then BreakdownValueLong_Type6 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="7">'  then BreakdownValueLong_Type7 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="8">'  then BreakdownValueLong_Type8 = input(scan(intext, 4,'<>= "'),comma24.0);
end;
if Section = "BreakN" then do;
          if XMLTag =: 'breakdownvalue type="1">'  then BreakdownValueNeither_Type1 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="2">'  then BreakdownValueNeither_Type2 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="3">'  then BreakdownValueNeither_Type3 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="4">'  then BreakdownValueNeither_Type4 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="5">'  then BreakdownValueNeither_Type5 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="6">'  then BreakdownValueNeither_Type6 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="7">'  then BreakdownValueNeither_Type7 = input(scan(intext, 4,'<>= "'),comma24.0);
          else if XMLTag =: 'breakdownvalue type="8">'  then BreakdownValueNeither_Type8 = input(scan(intext, 4,'<>= "'),comma24.0);
end;

if Section = "D" then do;
        if XMLTag =: 'securityname>'                        then   SecurityName   = scan(intext,-1,'<>');
   else if XMLTag =: 'localname>'                           then   LocalName      = scan(intext,-1,'<>');
   else if XMLTag =: 'externalname>'                        then  ExternalName     = scan(intext,-1,'<>');
   else if XMLTag =: 'country _id'                          then  do;
           Country     = scan(intext,-1,'<>');
           Country_ID  = scan(intext, 2,'"<>=');
   end;
   else if XMLTag =: 'currency _id'                          then  do;
           Currency    = scan(intext,-1,'<>');
           Currency_Id = scan(intext, 2,'"<>=');
   end;
   else if XMLTag =: 'cusip>'                               then  CUSIP     = scan(intext,-1,'<>');
   else if XMLTag =: 'sedol>'                               then  SEDOL     = scan(intext,-1,'<>');
   else if XMLTag =: 'isin>'                                then  ISIN     = scan(intext,-1,'<>');
   else if XMLTag =: 'ticker>'                              then  Ticker     = scan(intext,-1,'<>');
   else if XMLTag =: 'securityname>'                        then  SecurityName     = scan(intext,-1,'<>');
   else if XMLTag =: 'localsecuritytype>'                   then  LocalSecurityType    = scan(intext,-1,'<>');
   else if XMLTag =: 'legaltype>'                           then  LegalType     = scan(intext,-1,'<>');
   else if XMLTag =: 'localname>'                           then  LocalName     = scan(intext,-1,'<>');
   else if XMLTag =: 'weighting>'                           then  Weighting     = scan(intext,-1,'<>');
   else if XMLTag =: 'numberofshare>'                       then  NumberOfShare     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'originalnumberofshare>'               then  OriginalNumberOfShare     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'sharepercentage>'                     then  SharePercentage     = scan(intext,-1,'<>');
   else if XMLTag =: 'numberofjointlyownedshare>'           then  NumberOfJointlyOwnedShare     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'marketvalue>'                         then  MarketValue     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'originalmarketvalue>'                 then  OriginalMarketValue   = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'economicexposure>'                    then  EconomicExposure   = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'accountingvalue>'                     then  AccountingValue   = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'accountinglocalvalue>'                then  AccountingLocalValue   = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'dealid>'                              then  DealId     = scan(intext,-1,'<>');
   else if XMLTag =: 'costbasis>'                           then  CostBasis     = scan(intext,-1,'<>');
   else if XMLTag =: 'sharechange>'                         then  ShareChange     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'sector>'                              then  Sector     = scan(intext,-1,'<>');
   else if XMLTag =: 'holdingytdreturn>'                    then  HoldingYTDReturn     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'maturitydate>'                        then  MaturityDate     = input(scan(intext,-1,'<>'),yymmdd10.);
   else if XMLTag =: 'accruedinterest>'                     then  AccruedInterest     = scan(intext,-1,'<>');
   else if XMLTag =: 'strikeprice>'                         then  StrikePrice     = scan(intext,-1,'<>');
   else if XMLTag =: 'coupon>'                              then  Coupon     = scan(intext,-1,'<>');
   else if XMLTag =: 'couponrate>'                          then  CouponRate     = scan(intext,-1,'<>');
   else if XMLTag =: 'coupondaycount>'                      then  CouponDayCount     = scan(intext,-1,'<>');
   else if XMLTag =: 'couponspread>'                        then  CouponSpread     = scan(intext,-1,'<>');
   else if XMLTag =: 'couponpaymentfreqency>'               then  CouponPaymentFreqency     = scan(intext,-1,'<>');
   else if XMLTag =: 'couponreferencerate>'                 then  CouponReferenceRate     = scan(intext,-1,'<>');
   else if XMLTag =: 'originalcouponreferencerate>'         then  OriginalCouponReferenceRate     = scan(intext,-1,'<>');
   else if XMLTag =: 'region>'                              then  Region     = scan(intext,-1,'<>');
   else if XMLTag =: 'creditquality>'                       then  CreditQuality     = scan(intext,-1,'<>');
   else if XMLTag =: 'duration>'                            then  Duration     = scan(intext,-1,'<>');
   else if XMLTag =: 'industryid>'                          then  IndustryId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'globalindustryid>'                    then  GlobalIndustryId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'globalsector>'                        then  GlobalSector     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'gicsindustryid>'                      then  GICSIndustryId     = scan(intext,-1,'<>');
   else if XMLTag =: 'localcurrencycode>'                   then  LocalCurrencyCode     = scan(intext,-1,'<>');
   else if XMLTag =: 'localmarketvalue>'                    then  LocalMarketValue     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'localmarketvalue _currencycode'       then do;
   LocalCurrencyCode     = scan(intext,3,'<>= "');
   LocalMarketValue     = input(scan(intext,4,'<>= "'),comma24.0);
   end;
   else if XMLTag =: 'zafassettype>'                        then  ZAFAssetType     = scan(intext,-1,'<>');
   else if XMLTag =: 'paymenttype>'                         then  PaymentType     = scan(intext,-1,'<>');
   else if XMLTag =: 'rule144aeligible>'                    then  Rule144AEligible     = scan(intext,-1,'<>');
   else if XMLTag =: 'altmintaxeligible>'                   then  AltMinTaxEligible     = scan(intext,-1,'<>');
   else if XMLTag =: 'secondarysectorid>'                   then  SecondarySectorId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'bloombergticker>'                     then  BloombergTicker     = scan(intext,-1,'<>');
   else if XMLTag =: 'isoexchangeid>'                       then  ISOExchangeId       = scan(intext,-1,'<>');
   else if XMLTag =: 'contractsize>'                        then  ContractSize       = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'contracteffectivedate>'               then  ContractEffectiveDate = input(scan(intext,-1,'<>'),yymmdd10.);
   else if XMLTag =: 'companyid>'                           then  CompanyId     = scan(intext,-1,'<>');
   else if XMLTag =: 'companyname>'                         then  CompanyName      = scan(intext,-1,'<>');
   else if XMLTag =: 'firstboughtdate>'                     then  FirstBoughtDate     = input(scan(intext,-1,'<>'),yymmdd10.);
   else if XMLTag =: 'mexicantipovalor>'                    then  MexicanTipoValor     = scan(intext,-1,'<>');
   else if XMLTag =: 'mexicanserie>'                        then  MexicanSerie     = scan(intext,-1,'<>');
   else if XMLTag =: 'mexicanemisora>'                      then  MexicanEmisora     = scan(intext,-1,'<>');
   else if XMLTag =: 'performanceid>'                       then  PerformanceId     = scan(intext,-1,'<>');
   else if XMLTag =: 'underlyingperformanceid>'             then  UnderlyingPerformanceId     = scan(intext,-1,'<>');
   else if XMLTag =: 'underlyingsecid>'                     then  UnderlyingSecId     = scan(intext,-1,'<>');
   else if XMLTag =: 'underlyingsecurityname>'              then  UnderlyingSecurityName     = scan(intext,-1,'<>');
   else if XMLTag =: 'lessthanoneyearbond>'                 then  LessThanOneYearBond     = scan(intext,-1,'<>');
   else if XMLTag =: 'lessthan92daysbond>'                  then  LessThan92DaysBond     = scan(intext,-1,'<>');
   else if XMLTag =: 'zafbondissuerclass>'                  then  ZAFBondIssuerClass     = scan(intext,-1,'<>');
   else if XMLTag =: 'indiancreditqualityclassification>'   then  IndianCreditQualityClassificatio     = scan(intext,-1,'<>');
   else if XMLTag =: 'indianindustryclassification>'        then  IndianIndustryClassification     = scan(intext,-1,'<>');
   else if XMLTag =: 'chileanissuerrut>'                    then  ChileanIssuerRut     = scan(intext,-1,'<>');
   else if XMLTag =: 'chileannemotecnico>'                  then  ChileanNemotecnico     = scan(intext,-1,'<>');
   else if XMLTag =: 'chileaninstrumenttype>'               then  ChileanInstrumentType     = scan(intext,-1,'<>');
   else if XMLTag =: 'rtsymbol>'                            then  RTSymbol     = scan(intext,-1,'<>');
   else if XMLTag =: 'rtroot>'                              then  RTRoot     = scan(intext,-1,'<>');
   else if XMLTag =: 'rtexchangeid>'                        then  RTExchangeId     = scan(intext,-1,'<>');
   else if XMLTag =: 'rtsecuritytype>'                      then  RTSecurityType     = scan(intext,-1,'<>');
   else if XMLTag =: 'surveyedweighting>'                   then  SurveyedWeighting     = scan(intext,-1,'<>');
   else if XMLTag =: 'osicode>'                             then  OSICode     = scan(intext,-1,'<>');
   else if XMLTag =: 'derivativetenor>'                     then  DerivativeTenor     = scan(intext,-1,'<>');
   else if XMLTag =: 'stylebox>'                            then  StyleBox     = scan(intext,-1,'<>');
   else if XMLTag =: 'symbol>'                              then  Symbol     = scan(intext,-1,'<>');
end;
format MaturityDate
       Date
       PreviousPortfolioDate
       ContractEffectiveDate
       FirstBoughtDate     yymmddn8.;
run;
quit;


***************************************************************;
* Export Data                                                 *;
***************************************************************;
proc export data = work.PortfolioSummary
   outfile = "&output.PortfolioSummary_&filename..dta"
   dbms = dta replace;
run;
quit;
proc export data = work.HoldingDetail
   outfile = "&output.HoldingDetail_&filename..dta"
   dbms = dta replace;
run;
quit;

