var jobconfig = {
  "jobs": [
    {
      "source": "ALTER session set nls_date_format = 'dd/mm/yyyy'",
      "destination": "positions",
      "destinationrowid": "DEALNO",
      "operation": ""
    },
    {
      "source": `
          SELECT *
          FROM (
            SELECT '<%=ASONDATE%>' asondate, os_asset_class asset_class, os_holding HOLDING, os_subsidiaryname subsidiaryname, os_bookname bookname, os_portfolio portfolio, costing_policy COSTING_POLICY, p.symbol SECURITYSYMBOL, os_security_desc security_desc, os_security_type security_type, (os_total_facevalue / os_total_quantity) FACEVALUE_PERUNIT, os_quantity quantity, os_cl_quantity cl_quantity, os_total_quantity total_quantity, os_cl_facevalue cl_facevalue, os_total_facevalue total_facevalue, os_wac wac, os_cl_bookvalue cl_bookvalue, os_total_bookvalue total_bookvalue, 0 revalwac, 0 REVAL_BOOKVALUE, SecAccrual.accruals, secaccrual.ACCRUAL_PER_DAY_UNIT, secaccrual.ACCRUALDATE accrual_date, ACCRUALSGLOBAL, ACCRUALLCYCURR, ACCRUALLCYRATE, ACCRUALLCY, ACCRUALSGROUP, ACCRUALSGROUPRATE, ACCRUALSGROUPCURR, ACCRUALSGLOBALCURR, ACCRUALSOSAMT, ACCRUALSGLOBALRATE, MTMDATE mtm_value_date, mtmprice * os_total_quantity mtm, MTMEXCHRATE, MTMLCY, DURATION, MODIFIEDDURATION, MTMGLOBAL, MTMGROUP, pv01, MTMGLOBALCURR, MTMGROUPCURR, MTMLCYCURR, MTMGLOBALRATE, MTMGROUPRATE, MTMLCYRATE, MTMPL, MTMPLLCY, MTMPLGLOBAL, MTMPLGROUP, NETACCRUALS, CONVEXITY, MTMYIELD, CREDITEXPOSURE, lastammortdate, secammort.AMMORT_PU, secammort.AMMORTIZATION, AMRT_GROUPRATE, AMRT_GROUPAMT, AMRT_GLOBALCURR, AMRT_GLOBALRATE, AMRT_GLOBALAMT, AMRT_GROUPCURR, AMRT_LCYAMT, AMRT_LCYRATE, AMRT_LCYCURR, decode(p.product, 'SECU', p.os_WAC - ((os_total_facevalue / os_total_quantity) + Nvl(Decode(alm.description, 'Irregular Redemption', premium, 'Irregular Cashflows', premium, REDEMPTION_PREMIUM), 0)), 0) premium_val, alm.TYPE, alm.REPORTING_ASSET_CLASS, alm.INTERESTMODE, alm.INTEREST, alm.ISSUER, p.os_issuername, (To_Date(alm.enddate) - To_Date('<%=ASONDATE%>')) residual, alm.ISIN_FACEVALUE, Nvl(Round((p.os_facevalue / decode(alm.isin_facevalue, 0, 1, alm.isin_facevalue)), 12), 0) ISIN_QTY, alm.isino_code isin, alm.CURRENCY, alm.description cashflow_type, CASE WHEN asset = 'Equity' AND product = 'MFTR' THEN To_Date('') ELSE ENDDATE END maturity, p.asset, sectorname sector, industryname industry, alm.category_subtype MIS_CATEGORY_NAME, p.portfolio portcode, p.pos_bookid bookid, p.pos_subsidiarycode SUBSIDIARYCODE, nvl(newmtmmethod, decode(product, 'SECU', 'Interpolation', 'Swaps')) mtmmethod, 'Securitywise' viewdata, os_workflow workflow, alm.contractcode, p.asset assetclass, 'dealsymbol.symbol' subproduct, alm.INTERESTDIRECTION, alm.DISCOUNTED, alm.contractdesc PRODUCT_DESCRIPTION, p.os_total_quantity OUTSTANDINGAMT, p.os_total_quantity OSQUANTITY, p.product PRODUCT_CODE, alm.COUPONAT, alm.DISCOUNTCURVE, alm.DAYCOUNT, alm.FRN_COMPOUNDING, alm.SETTLEMENTCONV, alm.MARKUP, alm.NOTIONALSTEPRULE, alm.RESETCALN, alm.SPREADCOMPOUNDING, alm.BENCHMARKID, alm.FRN_RESET_UNIT, alm.COUPYEAR, alm.CALCULATIONPERIOD, alm.RESETCALCTYPE, alm.FRN_RESETTYPE, alm.FRN_FLR, alm.FRN_CAP, alm.BEGN_STUB, alm.BEGN_STUBDATE, alm.BEGN_STUBIN, alm.BEGN_REFERENCEBENCHMARK, alm.BEGN_STUBVALUE, alm.END_STUB, alm.END_STUBDATE, alm.END_STUBIN, alm.END_REFERENCEBENCHMARK, alm.END_STUBVALUE, alm.ONMONTHENDS, alm.SHUT_PERIOD, alm.EOMADHERENCE, alm.CAPS, alm.FLOORS, alm.RANGEBARRIERS, alm.DESCRIPTION, alm.ENDDATE MATDATE, alm.STARTDATE, alm.ENDDATE, alm.INCLUDEIPDATE, alm.REDEMPTION_PREMIUM, alm.HEDGEON, alm.RATING1, p.os_osunit OSUNIT, alm.CP_EOM_CURRENTDATE, alm.DEPOSITORY_ID, alm.INCLUDE_LAST_IP_DATE, alm.CP_INCLUDE_ALL_IP_DATE, alm.CP_INCLUDE_IP_DATE, alm.INCLUDE_ALL_IP_DATE, p.event TRANSACTION_TYPE_CODE, 'NA' SELECTIONFLAG, alm.INT_ACC_IPDATE, nvl(exchangerate.mid, 1) exchrate, alm.newuploadkey uploadkey, '' EVENT, alm.ACCOUNTNO, alm.BANKCODE, alm.EARLYTERMINATION, alm.CANCELLABLE, alm.SPOTDATE, alm.RESETDATE, alm.FRN_RESET, alm.FRN_COMPOUNDING_TYPE, ALM.FRN, alm.YIELD_TYPE, alm.UNADJUSTEDENDDATE, alm.TRADEDATE, decode(Lower(p.os_workflow), 'security', decode(description, 'Discounted', 'Accretion', 'Ammortization'), 'NA') RESULT_FOR, alm.MANUAL_CASHFLOW, p.posamt_groupcurr GROUPCURR_CLEAR_AMT, p.POSAMT_GROUPCURR, p.GLOBALCURR, p.POSAMT_GLOBALCURR, posamt_globalcurr GLOBALCURR_CLEAR_AMT, p.GROUPCURR, p.LOCALCURR, p.POSAMT_LCY, posamt_lcy LCY_CLEAR_AMT, p.os_total_facevalue FACEVALUE, p.SYMBOL, alm.NPI_STATUS, alm.NPI_EFFECTIVE_DATE, alm.LISTED, p.os_clear_qty CLEAR_QTY, p.os_bookvalue pos_bookvalue, p.os_cl_facevalue CLEARFACEVALUE, p.os_cl_bookvalue CLEAR_BOOKVALUE, Decode(Upper(listed), 'TRUE', 1, 'FALSE', 0) listingstatus, Decode(Upper(p.os_workflow), 'FUTURES', Decode(Sign(p.POS_BOOKVALUE), - 1, 'SECURITY IN HAND - SHORT', 'SECURITY IN HAND'), os_HOLDING) HOLDINGTYPE, To_Date(enddate) - To_Date('<%=ASONDATE%>') RES_MATURITY, alm.ISSUER_GROUP, alm.CAR_PERC, alm.NETWORTH, alm.illiquid LIQUIDITYSTATUS, alm.ISSUER_RATING, alm.ISSUER_GRP_RATING, alm.REGULATORY_CATEGORY, alm.RATED_AMOUNT, CASE WHEN asset = 'Equity' AND product = 'MFTR' THEN 0 ELSE Round((To_Date(enddate) - To_Date('<%=ASONDATE%>')) / 365, 2) END AS RESIDUAL_IN_YRS, Decode(listed, 'True', 'Listed', 'False', 'UnListed', listed) LISTING_CATEGORY, alm.sec_remarks DEAL_TYPE, p.os_holding HOLDINGFOROPTM, (nvl(p.os_total_bookvalue, 0) * alm.interestdirection) NET_MM_BORROWING, alm.SEC_DEAL_LOC, CASE WHEN p.os_workflow = 'Options' AND To_Date('<%=ASONDATE%>', 'dd/MM/yyyy') >= To_Date(ENDDATE, 'dd/MM/yyyy') THEN 'True' ELSE 'False' END OPTREDEM, Decode(p.os_holding, 'SECURITY IN SHORT', nvl(p.os_total_facevalue, 0), nvl(p.os_total_facevalue, 0) * - 1) net_short_position, to_date('', 'dd/MM/yyyy') LASTIPDATE, to_date('', 'dd/MM/yyyy') IPSTARTDATE, to_date('', 'dd/MM/yyyy') NEXTINTDATE, 0 INFLATION_ADJ_FACE_VALUE, 0 INDEX_RATIO, 0 INFLATION_ADJ_CL_FACE_VALUE, Nvl(secaccrual.taxrate, 0) TAXRATE, Nvl(secaccrual.GROSSAMT, 0) GROSSAMT, Nvl(secaccrual.TAXAMT, 0) TAXAMT, decode(product, 'IRSC', '8', 'LOAN', '6') WORKFLOWCODE, '' LASTISSUEDATE, '' NEXTRESETDATE, To_Date('<%=ASONDATE%>') || p.symbol || p.os_subsidiaryname || p.os_bookname || p.os_portfolio || p.os_holding eventkey, (CASE WHEN p.os_HOLDING = 'WHEN ISSUED PURCHASE APPLICATION' THEN GETNOTIFIEDAMT(p.SYMBOL, '<%=ASONDATE%>') WHEN p.os_HOLDING = 'WHEN ISSUED PURCHASE APPLICATION' THEN GETNOTIFIEDAMT(p.SYMBOL, '<%=ASONDATE%>') ELSE 0 END) NOTIFIED_AMOUNT, alm.payment_mode PAYMENT_MODE, notch RATING_NOTCH, p.os_issuername issuername, mtmprice price, p.
              os_yield YIELD, alm.mm_workflow, pm.isconstituent isconstituent
            FROM positions p, portmast pm, (
                SELECT *
                FROM swapaccruals
                WHERE acc_product IN ('Security', 'Loan', 'Swaps') AND accrualdate = (
                    SELECT Max(accrualdate) accrualdate
                    FROM swapaccruals
                    WHERE acc_product IN ('Security', 'Loan', 'Swaps') AND trunc(accrualdate) = to_date('<%=ASONDATE%>')
                    )
                ) SecAccrual, (
                SELECT securitysymbol, mtmdate, mtmvalue, mtmmethod, netaccruals, netaccrualspv, contractcode, entrytype, pv01, creditexposure, mtmexchrate, mtmlcy, duration, modifiedduration, systemvalue, remarks, delta, gamma, vega, theta, rho_curr1, rho_curr2, margin, mtmglobal, mtmgroup, mtmglobalcurr, mtmgroupcurr, mtmlcycurr, mtm_assetclass, mtmprice, mtmglobalrate, mtmgrouprate, mtmlcyrate, mtmpl, mtmpllcy, mtmplglobal, mtmplgroup, mtm_subsidiary, mtm_bookid, mtm_product, mtm_portfolio, mtm_curr, mtm_viewdata, mtmyield, convexity, mtm_bookvalue, mtm_workflow, mtm_optiontype, dealbucket, pv_factor, revised_pl, interpolated_benchmark_rate, spot_mtmrate, spot_mtmamount, spot_mtmpl, record_id, Decode(MTM_WORKFLOW, 'Options', CASE WHEN holding_type = 'Short Call' OR holding_type = 'Short Put' THEN 'SECURITY IN HAND - SHORT' WHEN holding_type = 'Long Call' OR holding_type = 'Long Put' THEN 'SECURITY IN HAND' ELSE holding_type END, holding_type) holding_type
                FROM (
                  SELECT SECURITYSYMBOL, MTMDATE, MTMVALUE, MTMMETHOD, NETACCRUALS, NETACCRUALSPV, CONTRACTCODE, ENTRYTYPE, PV01, CREDITEXPOSURE, MTMEXCHRATE, MTMLCY, DURATION, MODIFIEDDURATION, SYSTEMVALUE, REMARKS, DELTA, GAMMA, VEGA, THETA, RHO_CURR1, RHO_CURR2, MARGIN, MTMGLOBAL, MTMGROUP, MTMGLOBALCURR, MTMGROUPCURR, MTMLCYCURR, MTM_ASSETCLASS, MTMPRICE, MTMGLOBALRATE, MTMGROUPRATE, MTMLCYRATE, MTMPL, MTMPLLCY, MTMPLGLOBAL, MTMPLGROUP, MTM_SUBSIDIARY, MTM_BOOKID, MTM_PRODUCT, MTM_PORTFOLIO, MTM_CURR, MTM_VIEWDATA, MTMYIELD, CONVEXITY, MTM_BOOKVALUE, MTM_WORKFLOW, MTM_OPTIONTYPE, DEALBUCKET, PV_FACTOR, REVISED_PL, INTERPOLATED_BENCHMARK_RATE, SPOT_MTMRATE, SPOT_MTMAMOUNT, SPOT_MTMPL, RECORD_ID, HOLDING_TYPE
                  FROM mtmvalues
                  WHERE mtm_product IN ('Security', 'Equity', 'Mutual Funds', 'Options', 'Alternative Investment') AND (mtmdate, contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio, holding_type) IN (
                      SELECT Max(mtmdate), contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio, holding_type
                      FROM mtmvalues
                      WHERE mtm_product IN ('Security', 'Equity', 'Mutual Funds', 'Options', 'Alternative Investment') AND trunc(mtmdate) <= to_date('<%=ASONDATE%>')
                      GROUP BY contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio, holding_type
                      )
                  )
      
                UNION ALL
      
                SELECT securitysymbol, mtmdate, mtmvalue, mtmmethod, netaccruals, netaccrualspv, contractcode, entrytype, pv01, creditexposure, mtmexchrate, mtmlcy, duration, modifiedduration, systemvalue, remarks, delta, gamma, vega, theta, rho_curr1, rho_curr2, margin, mtmglobal, mtmgroup, mtmglobalcurr, mtmgroupcurr, mtmlcycurr, mtm_assetclass, mtmprice, mtmglobalrate, mtmgrouprate, mtmlcyrate, mtmpl, mtmpllcy, mtmplglobal, mtmplgroup, mtm_subsidiary, mtm_bookid, mtm_product, mtm_portfolio, mtm_curr, mtm_viewdata, mtmyield, convexity, mtm_bookvalue, mtm_workflow, mtm_optiontype, dealbucket, pv_factor, revised_pl, interpolated_benchmark_rate, spot_mtmrate, spot_mtmamount, spot_mtmpl, record_id, Decode(holding_type, 'Short Futures', 'SECURITY IN HAND - SHORT', 'SECURITY IN HAND') holding_type
                FROM (
                  SELECT contractcode SECURITYSYMBOL, MTMDATE, 0 MTMVALUE, '' MTMMETHOD, 0 NETACCRUALS, 0 NETACCRUALSPV, CONTRACTCODE, '' ENTRYTYPE, 0 PV01, 0 CREDITEXPOSURE, 0 MTMEXCHRATE, 0 MTMLCY, 0 DURATION, 0 MODIFIEDDURATION, 0 SYSTEMVALUE, '' REMARKS, 0 DELTA, 0 GAMMA, 0 VEGA, 0 THETA, 0 RHO_CURR1, 0 RHO_CURR2, 0 MARGIN, '' MTMGLOBAL, '' MTMGROUP, '' MTMGLOBALCURR, '' MTMGROUPCURR, '' MTMLCYCURR, MTM_ASSETCLASS, Avg(MTMPRICE) MTMPRICE, 0 MTMGLOBALRATE, 0 MTMGROUPRATE, 0 MTMLCYRATE, 0 MTMPL, 0 MTMPLLCY, 0 MTMPLGLOBAL, 0 MTMPLGROUP, MTM_SUBSIDIARY, MTM_BOOKID, MTM_PRODUCT, MTM_PORTFOLIO, MTM_CURR, '' MTM_VIEWDATA, 0 MTMYIELD, 0 CONVEXITY, 0 MTM_BOOKVALUE, '' MTM_WORKFLOW, '' MTM_OPTIONTYPE, '' DEALBUCKET, 0 PV_FACTOR, 0 REVISED_PL, 0 INTERPOLATED_BENCHMARK_RATE, 0 SPOT_MTMRATE, 0 SPOT_MTMAMOUNT, 0 SPOT_MTMPL, 0 RECORD_ID, holding_type
                  FROM mtmvalues
                  WHERE mtm_product = 'Futures' AND (mtmdate, contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio) IN (
                      SELECT Max(mtmdate), contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio
                      FROM mtmvalues
                      WHERE mtm_product = 'Futures' AND trunc(mtmdate) <= to_date('<%=ASONDATE%>')
                      GROUP BY contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio
                      )
                  GROUP BY contractcode, mtm_subsidiary, mtm_bookid, mtm_assetclass, mtm_product, mtm_portfolio, mtm_product, mtm_curr, MTMDATE, holding_type
                  )
                ) secmtm, holding_wac hw, (
                SELECT *
                FROM ammortization
                WHERE (ammortdate, AMRT_SUBSIDIARYNAME, AMRT_BOOKNAME, AMRT_PORTFOLIO, SECURITYSYMBOL) IN (
                    SELECT Max(ammortdate), AMRT_SUBSIDIARYNAME, AMRT_BOOKNAME, AMRT_PORTFOLIO, SECURITYSYMBOL
                    FROM ammortization
                    WHERE trunc(AMMORTDATE) <= to_date( '<%=ASONDATE%>')
                    GROUP BY AMRT_SUBSIDIARYNAME, AMRT_BOOKNAME, AMRT_PORTFOLIO, SECURITYSYMBOL
                    )
                ) SecAmmort, (
                SELECT contractcode, max(ammortdate) LastAmmortDate
                FROM ammortization
                WHERE ammortdate <= '<%=ASONDATE%>'
                GROUP BY contractcode
                ) LastAmmortDate, allmasters alm, (
                SELECT *
                FROM spotratesources sp, exchangerate e
                WHERE trunc(exchangedate) = to_date('<%=ASONDATE%>', 'dd/MM/yyyy') AND e.source = sp.spotratesources_id
                ) ExchangeRate, (
                SELECT Sum(premium) premium, securitysymbol
                FROM securityevents
                WHERE ACTUALDATE >= '<%=ASONDATE%>' AND EVENTDESCRIPTION = 'Partial Redemption'
                GROUP BY securitysymbol
                ) premium
            WHERE nvl(lastdeal, 0) = 1 AND p.portfolio = pm.portcode AND Nvl(deletionstatus, 'ND') = 'ND' AND alm.securitysymbol(+) = p.symbol AND lower(product) NOT IN ('cash') AND lower(asset) IN ('interest rate', 'security', 'equity', 'mutual funds', 'currency', 'alternative investment') AND p.holding = hw.ORIGINAL_HOLDING_TYPE(+) AND p.symbol = SecAccrual.contractcode(+) AND p.symbol = SecMTM.contractcode(+) AND p.os_subsidiaryname = SecMTM.mtm_subsidiary(+) AND p.os_bookname = SecMTM.mtm_bookid(+) AND p.asset = SecMTM.mtm_assetclass(+) AND p.os_workflow = SecMTM.mtm_product(+) AND p.os_portfolio = SecMTM.mtm_portfolio(+) AND p.os_holding = SecMTM.holding_type(+) AND p.symbol = SecAmmort.contractcode(+) AND p.os_subsidiaryname = SecAmmort.amrt_subsidiaryname(+) AND p.os_bookname = secammort.amrt_bookname(+) AND p.os_portfolio = secammort.amrt_portfolio(+) AND LastAmmortDate.contractcode(+) = p.symbol AND p.os_subsidiaryname = SecAccrual.acc_subsidiary(+) AND p.os_bookname = SecAccrual.acc_bookid(+) AND p.asset = SecAccrual.acc_assetclass(+) AND p.os_workflow = SecAccrual.acc_product(+) AND p.os_portfolio = SecAccrual.acc_portfolio(+) AND alm.Currency = ExchangeRate.base(+) AND ExchangeRate.derive(+) = 'INR' AND PREMIUM.securitysymbol(+) = alm.securitysymbol AND p.os_total_quantity > 0
            )
          WHERE 1 = 1 AND holding <> 'ORDER MANDATE' AND SECURITYSYMBOL = '<%=SECURITYSYMBOL%>' AND SUBSIDIARYNAME = '<%=SUBSIDIARYNAME%>' AND BOOKNAME = '<%=BOOKNAME%>' AND PORTFOLIO = '<%=PORTNAME%>'
      `,
      "destination": "positions",
      "destinationrowid": ["ASONDATE", "SUBSIDIARYNAME", "BOOKNAME"],
      "operation": "refresh"
    }
  ]
}
module.exports = jobconfig;