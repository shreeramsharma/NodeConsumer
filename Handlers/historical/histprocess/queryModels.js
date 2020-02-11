let callRest = funclib['callRest'];
let updateOraDB = funclib['updateOraDB']

module.exports = {
    position: async function (asondate, sessionid) {
        try {
            var querySet = []
            let positionUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V200&event=getPositionsRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let positionQuery = await callRest(positionUrl)
            if (positionQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_trade_position WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_trade_position(asondate, portfolio, asset_class_name, scheme_type, isin_no, base_curr_symbol, securitysymbol, security, holding_type, quantity, facevalue, index_ratio, wac, batch_price, batch_no, infla_adj_facevalue, trade_date, bookvalue, ammortised_price, ammortised_bookvalue, market_price, market_vallue, notional_pl, pcy_mtm_pl, pcy_currency_pl, pcy_notional_pl, coupon, way, mtm_yield, duration, modified_duration, convexity, pvbp, mfoption, issuer_name, industry_name, group_name, promoter_group, fimmda_sector, portfolio_group1, portfolio_group2, portfolio_group3, portfolio_group4, portfolio_group5, portfolio_group6, portfolio_group7, irda_category_code, irda_category_description, irda_category_grp_desc, ratingagency1, rating1, ratingagency2, rating2, ratingagency3, rating3, putcalldate, maturity_date, face_value_per_unit, redemption_premium, cashflowtype, portfolio_code, external_code, external_sub_portfolio_code, day_count, direction, no_of_contracts, contracttype, underlying_security, contractoption, lot_size, settlement_option, strike_price, performing_status, performing_sub_status, local_currency, lcy_rate, lcy_bookvalue, lcy_mtmvalue, lcy_amortised_bv, global_currency, gcy_rate, gcy_bookvalue, gcy_mtmvalue, gcy_amortised_bv, portfolio_currency, pcy_rate, pcy_bookvalue, pcy_mtmvalue, pcy_amortised_bv, entity_name, asset_class_code, reporting_asset_class, dealer_id, isin_facevalue, isin_quantity, fund_code, fund_name, transaction_type, transaction_type_code, ifrs_category, book_value_annualized_yield, market_value_annualized_yield, security_status, frequency, effective_date, ratingdate1, portfolio_duration, residual_tenor, redemption_premium_value, type_of_way, penalty_waiver, division_code, division_name, secured)"
                querySet[querySet.length] = ` select '${asondate}' asondate, portfolio, asset_class_name, scheme_type, isin_no, base_curr_symbol, securitysymbol, security, holding_type, quantity, facevalue, index_ratio, wac, batch_price, batchno AS batch_no, infla_adj_facevalue,TRANSDATE as trade_date, bookvalue, ammortised_price, ammortised_bookvalue,MTMPRICE as market_price,MTMVALUE as market_vallue, notional_pl, pcy_mtm_pl, pcy_currency_pl, pcy_notional_pl, coupon, way,MTMYIELD as mtm_yield, duration, MODIFIEDDURATION AS modified_duration, convexity, pvbp, mfoption,ISSUER AS issuer_name,INDUSTRY AS industry_name, group_name,PROMOTER_GRP AS promoter_group, fimmda_sector,group1 AS  portfolio_group1,group2 AS  portfolio_group2,group3 as portfolio_group3,group4 AS  portfolio_group4,group5 AS  portfolio_group5,group6 AS  portfolio_group6,group7 AS  portfolio_group7, CATCODE AS irda_category_code, CATGRPDESC AS irda_category_description, CATDESC AS irda_category_grp_desc, ratingagency1, rating1, ratingagency2, rating2, ratingagency3, rating3, putcalldate, MATURITYDATE AS maturity_date, face_value_per_unit, redemption_premium, cashflowtype, PORTCODE AS portfolio_code, external_code, EXTERNAL_SUB_PORTCODE AS external_sub_portfolio_code, day_count, direction, no_of_contracts, contracttype,  underlying AS underlying_security, contractoption, lotsize as lot_size, settlementoption AS settlement_option, STRIKEPRICE AS strike_price, performing_status, performing_sub_status, lcy AS local_currency, lcy_rate, lcy_bookvalue, lcy_mtmvalue, lcy_amortised_bv, GCY AS global_currency, gcy_rate, gcy_bookvalue, gcy_mtmvalue, gcy_amortised_bv, PCY AS portfolio_currency, pcy_rate, pcy_bookvalue, pcy_mtmvalue, pcy_amortised_bv, ENTITYNAME AS entity_name, asset_class_code, REPORTINGASSETCLASS AS reporting_asset_class, dealer_id, isin_facevalue_pu AS  isin_facevalue, isin_quantity, fund_code, fund_name, transactiontype AS transaction_type , transaction_type_code, ifrs_category, book_value_annualized_yield, market_value_annualized_yield, security_status, frequency, effective_date, ratingdate1, portfolio_duration, residual_tenor, redemption_premium_value, type_of_way, penalty_waiver, DIVISIONCODE AS division_code, DIVISIONNAME AS division_name, secured from (`
                querySet[querySet.length] = positionQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("Position: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`Position: Error: ${error}`);
        }
    },
    deallisting: async function (asondate, sessionid) {
        try {
            var querySet = []
            let deallistingUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V201&event=getDeallistingRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let deallistingQuery = await callRest(deallistingUrl)
            if (deallistingQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_deallisting WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_deallisting(ASONDATE, PAYMENT_MODE, ASSET_CLASS_CODE, SECURITYSYMBOL, DEAL_STATUS, SOFTLIMITBREACHED, PORTFOLIO, FUND_CODE, FUND_NAME, ASSET_CLASS_NAME, TYPE, DEAL_NO, DEALER_TICKET_NO, DEALER, TRANSACTION_TYPE, TRADE_DATE, SETTLEMENT_DATE, BACK_VALUE_DATE, SECURITY, SECURITY_CURRENCY, COUNTERPARTY, QUANTITY, FACEVALUE, PRICE, AMOUNT, ACCRUED_INTEREST_PU, ACCRUED_INTEREST_AMOUNT, INTEREST, YIELD, TENOR, CHARGES, NET_CONSIDERATION, INDEX_RATIO, NOMINAL_PRICE, INFLATION_ADJ_PRINCIPAL, NOMINAL_ACCRUED_INTEREST, INFLATION_ADJ_NET_CON, WITH_HOLDING_TAX, PENALTY_INTEREST_RATE, PENALTY_INTEREST, SECOND_LEG_VALUE_DATE, SECOND_LEG_PRICE, SECOND_LEG_AMOUNT, ACCRUED_INTEREST_SECOND_LEG, SECOND_LEG_ACCRUED_INT_PU, SECOND_LEG_YTM, REPO_RATE, BROKER_NAME, EXCHANGE, FOLIONUMBER, INT_SCHEME_PORTFOLIO_NO, REF_DEAL_TICKET_NO, REMARKS, INTEREST_ON_FD, AUTHORISED_BY, AUTHORISATION_DATETIME, CONFIRMED_BY, CONFIRMATION_TIMESTAMP, BANK_SETTLED_STATUS, IS_DEAL_UPLOADED, UPLOAD_TRADENO, GUID, TRANSACTION_TYPE_CODE, PORTFOLIO_CODE, CONTRACT_CODE, UNDERLYING_SECURITY, CONTRACT_OPTION, LOT_SIZE, SETTLEMENT_OPTION, STRIKE_PRICE, OLD_SECURITY_SYMBOL, REGULATORY_CATCODE, IRDA_SUB_CATEGORY, IRDA_MAIN_CATEGORY, PORTFOLIO_GROUP1, PORTFOLIO_GROUP2, PORTFOLIO_GROUP3, PORTFOLIO_GROUP4, PORTFOLIO_GROUP5, PORTFOLIO_GROUP6, PORTFOLIO_GROUP7, GROUP_NAME, DEAL_STATUS_CODE, ENTITY_NAME, GCY_RATE, GCY_AMOUNT, LYC_RATE, LYC_AMOUNT, PCY_RATE, PCY_AMOUNT, CUSTOMER_ID, BANK_BRANCH, DEBIT_ACCOUNT_NUMBER, BENEFICIARY_NAME, BANK_NAME, CURRENT_ACCOUNT_NUMBER, BRANCH_NAME, REPORTING_ASSET_CLASS, DISCOUNT_RATE, ISIN_FACEVALUE, ISIN_QUANTITY, GROSS_INTEREST, TDSAMOUNT, ORIGINALINTEREST, OTHERINTEREST, INCOME_DAYCOUNT_BASIS, ISSUE_TYPE, SLB_MARGIN_PERCENT, SLB_MARGIN_AMOUNT, SLB_FEE_PERCENTAGE, FEE_RECEIVED_PAID, SLB_WAY, OLD_SYMBOL, SLB_TENOR, FDRNO, SETTLEMENT_AC_NAME, AMC_ACCOUNT_NAME, MATURITY_DATE, ISIN_NO, COSTING_TYPE, REVERSED, BACKDATED_AMMORTISATION, BACKDATED_ACCRUAL, ORDER_ID, PROFIT, PREORDER_ID)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE, PAYMENT_MODE, ASSET_CLASS_CODE, SECURITYSYMBOL, DEAL_STATUS, SOFTLIMITBREACHED, PORTFOLIO, FUND_CODE, FUND_NAME, ASSET_CLASS_NAME, TYPE, DEAL_NO, DEALER_TICKET_NO, DEALER, TRANSACTION_TYPE, TRADE_DATE,settlementdate SETTLEMENT_DATE, BACK_VALUE_DATE,securitysymbol SECURITY,SECURITY_CURRENCY, COUNTERPARTY, QUANTITY, FACEVALUE, PRICE, AMOUNT, ACCRUED_INTEREST_PU, ACCRUED_INTEREST_AMOUNT, INTEREST, YIELD, TENOR, CHARGES, NET_CONSIDERATION, INDEX_RATIO, NOMINAL_PRICE, INFLATION_ADJ_PRINCIPAL, NOMINAL_ACCRUED_INTEREST, INFLATION_ADJ_NET_CON, WITH_HOLDING_TAX,penalty_interest PENALTY_INTEREST_RATE, PENALTY_INTEREST, SECOND_LEG_VALUE_DATE, SECOND_LEG_PRICE, SECOND_LEG_AMOUNT, ACCRUED_INTEREST_SECOND_LEG, SECOND_LEG_ACCRUED_INT_PU, SECOND_LEG_YTM,REPORATE REPO_RATE,broker BROKER_NAME, EXCHANGE, FOLIONUMBER, INT_SCHEME_PORTFOLIO_NO, REF_DEAL_TICKET_NO, REMARKS, INTEREST_ON_FD,authorizedby AUTHORISED_BY, authorizationdate AUTHORISATION_DATETIME, confirmedby CONFIRMED_BY, CONFIRMATION_TIMESTAMP, BANK_SETTLED_STATUS,isuploaded IS_DEAL_UPLOADED, UPLOAD_TRADENO, GUID, TRANSACTION_TYPE_CODE, PORTFOLIO_CODE, CONTRACT_CODE,underlying UNDERLYING_SECURITY,CONTRACT_OPTION, LOT_SIZE, SETTLEMENT_OPTION, STRIKE_PRICE, OLD_SECURITY_SYMBOL, REGULATORY_CATCODE, IRDA_SUB_CATEGORY, IRDA_MAIN_CATEGORY,GROUP1 PORTFOLIO_GROUP1,GROUP2 PORTFOLIO_GROUP2,GROUP3 PORTFOLIO_GROUP3,GROUP4 PORTFOLIO_GROUP4,GROUP5 PORTFOLIO_GROUP5,GROUP6 PORTFOLIO_GROUP6,GROUP7 PORTFOLIO_GROUP7, GROUP_NAME, DEAL_STATUS_CODE,entityname ENTITY_NAME, GCY_RATE, GCY_AMOUNT, LYC_RATE, LYC_AMOUNT, PCY_RATE, PCY_AMOUNT, CUSTOMER_ID, BANK_BRANCH, DEBIT_ACCOUNT_NUMBER, BENEFICIARY_NAME, BANK_NAME, CURRENT_ACCOUNT_NUMBER,branch BRANCH_NAME,reportingassetclass REPORTING_ASSET_CLASS, DISCOUNT_RATE,isin_facevalue_pu ISIN_FACEVALUE, ISIN_QUANTITY, GROSS_INTEREST, TDSAMOUNT, ORIGINALINTEREST, OTHERINTEREST, INCOME_DAYCOUNT_BASIS, ISSUE_TYPE,slb_margin_per SLB_MARGIN_PERCENT, slb_margin_amt SLB_MARGIN_AMOUNT,slb_fee_per SLB_FEE_PERCENTAGE, FEE_RECEIVED_PAID, SLB_WAY, OLD_SYMBOL, SLB_TENOR, FDRNO, SETTLEMENT_AC_NAME,amc_ac_name as AMC_ACCOUNT_NAME,  matdate AS MATURITY_DATE, rbi_symbol AS ISIN_NO, COSTING_TYPE, REVERSED, BACKDATED_AMMORTISATION, BACKDATED_ACCRUAL, ORDER_ID, PROFIT, PREORDER_ID from (`
                querySet[querySet.length] = deallistingQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("Deallisting: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`Deallisting: Error: ${error}`);
        }
    },
    settleposition: async function (asondate, sessionid) {
        try {
            var querySet = []
            let settlePositionUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V202&event=getPositionssettleRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let settlePositionQuery = await callRest(settlePositionUrl)
            if (settlePositionQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM HIS_SETTLE_POSITION WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO HIS_SETTLE_POSITION(ASONDATE, PORTFOLIO, ASSET_CLASS_NAME, SCHEME_TYPE, ISIN_NO, BASE_CURR_SYMBOL, SECURITYSYMBOL, SECURITY, HOLDING_TYPE, QUANTITY, FACEVALUE, INDEX_RATIO, WAC, BATCH_PRICE, BATCH_NO, INFLA_ADJ_FACEVALUE, TRADE_DATE, BOOKVALUE, AMMORTISED_PRICE, AMMORTISED_BOOKVALUE, MARKET_PRICE, MARKET_VALUE, NOTIONAL_PL, PCY_MTM_PL, PCY_CURRENCY_PL, PCY_NOTIONAL_PL, COUPON, WAY, MTM_YIELD, DURATION, MODIFIED_DURATION, CONVEXITY, PVBP, MFOPTION, ISSUER_NAME, INDUSTRY_NAME, GROUP_NAME, PROMOTER_GROUP, FIMMDA_SECTOR, PORTFOLIO_GROUP1, PORTFOLIO_GROUP2, PORTFOLIO_GROUP3, PORTFOLIO_GROUP4, PORTFOLIO_GROUP5, PORTFOLIO_GROUP6, PORTFOLIO_GROUP7, IRDA_CATEGORY_CODE, IRDA_CATEGORY_DESCRIPTION, IRDA_CATEGORY_GRP_DESC, RATINGAGENCY1, RATING1, RATINGAGENCY2, RATING2, RATINGAGENCY3, RATING3, PUTCALLDATE, MATURITY_DATE, FACE_VALUE_PER_UNIT, REDEMPTION_PREMIUM, CASHFLOWTYPE, PORTFOLIO_CODE, EXTERNAL_CODE, EXTERNAL_SUB_PORTFOLIO_CODE, DAY_COUNT, DIRECTION, NO_OF_CONTRACTS, CONTRACTTYPE, UNDERLYING_SECURITY, CONTRACTOPTION, LOT_SIZE, SETTLEMENT_OPTION, STRIKE_PRICE, PERFORMING_STATUS, PERFORMING_SUB_STATUS, LOCAL_CURRENCY, LCY_RATE, LCY_BOOKVALUE, LCY_MTMVALUE, LCY_AMORTISED_BV, GLOBAL_CURRENCY, GCY_RATE, GCY_BOOKVALUE, GCY_MTMVALUE, GCY_AMORTISED_BV, PORTFOLIO_CURRENCY, PCY_RATE, PCY_BOOKVALUE, PCY_MTMVALUE, PCY_AMORTISED_BV, ENTITY_NAME, ASSET_CLASS_CODE, REPORTING_ASSET_CLASS, DEALER_ID, ISIN_FACEVALUE, ISIN_QUANTITY, FUND_CODE, FUND_NAME, TRANSACTION_TYPE, TRANSACTION_TYPE_CODE, IFRS_CATEGORY, BOOK_VALUE_ANNUALIZED_YIELD, MARKET_VALUE_ANNUALIZED_YIELD, SECURITY_STATUS, FREQUENCY, EFFECTIVE_DATE, RATINGDATE1, PORTFOLIO_DURATION, RESIDUAL_TENOR, REDEMPTION_PREMIUM_VALUE, TYPE_OF_WAY, PENALTY_WAIVER, DIVISION_CODE, DIVISION_NAME, SECURED)"
                querySet[querySet.length] = ` select '${asondate}' asondate, portfolio, asset_class_name, scheme_type, isin_no, base_curr_symbol, securitysymbol, security, holding_type, quantity, facevalue, index_ratio, wac, batch_price, batchno AS batch_no, infla_adj_facevalue,TRANSDATE as trade_date, bookvalue, ammortised_price, ammortised_bookvalue,MTMPRICE as market_price,MTMVALUE as market_vallue, notional_pl, pcy_mtm_pl, pcy_currency_pl, pcy_notional_pl, coupon, way,MTMYIELD as mtm_yield, duration, MODIFIEDDURATION AS modified_duration, convexity, pvbp, mfoption,ISSUER AS issuer_name,INDUSTRY AS industry_name, group_name,PROMOTER_GRP AS promoter_group, fimmda_sector,group1 AS  portfolio_group1,group2 AS  portfolio_group2,group3 as portfolio_group3,group4 AS  portfolio_group4,group5 AS  portfolio_group5,group6 AS  portfolio_group6,group7 AS  portfolio_group7, CATCODE AS irda_category_code, CATGRPDESC AS irda_category_description, CATDESC AS irda_category_grp_desc, ratingagency1, rating1, ratingagency2, rating2, ratingagency3, rating3, putcalldate, MATURITYDATE AS maturity_date, face_value_per_unit, redemption_premium, cashflowtype, PORTCODE AS portfolio_code, external_code, EXTERNAL_SUB_PORTCODE AS external_sub_portfolio_code, day_count, direction, no_of_contracts, contracttype,  underlying AS underlying_security, contractoption, lotsize as lot_size, settlementoption AS settlement_option, STRIKEPRICE AS strike_price, performing_status, performing_sub_status, lcy AS local_currency, lcy_rate, lcy_bookvalue, lcy_mtmvalue, lcy_amortised_bv, GCY AS global_currency, gcy_rate, gcy_bookvalue, gcy_mtmvalue, gcy_amortised_bv, PCY AS portfolio_currency, pcy_rate, pcy_bookvalue, pcy_mtmvalue, pcy_amortised_bv, ENTITYNAME AS entity_name, asset_class_code, REPORTINGASSETCLASS AS reporting_asset_class, dealer_id, isin_facevalue_pu AS  isin_facevalue, isin_quantity, fund_code, fund_name, transactiontype AS transaction_type , transaction_type_code, ifrs_category, book_value_annualized_yield, market_value_annualized_yield, security_status, frequency, effective_date, ratingdate1, portfolio_duration, residual_tenor, redemption_premium_value, type_of_way, penalty_waiver, DIVISIONCODE AS division_code, DIVISIONNAME AS division_name, secured FROM (`
                querySet[querySet.length] = settlePositionQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("SettlePosition: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`SettlePosition: Error: ${error}`);
        }
    },
    bankbalances: async function (asondate, sessionid) {
        try {
            var querySet = []
            let bankBalancesUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V203&event=getBankBalancesViewRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let bankBalancesQuery = await callRest(bankBalancesUrl)
            if (bankBalancesQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_bankbalances WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_bankbalances (ASONDATE, BANK_ACCOUNT_ID, BANK_CODE, BANK_NAME, AC_NO, BANK_ACCOUNT, BANK_ACCOUNT_TYPE, CURR_SYMBOL, PORTFOLIO_CODE, PORTFOLIO_NAME, AVAILABLE_BALANCE)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE,BANKACID AS BANK_ACCOUNT_ID,BANKCODE AS BANK_CODE,BANKNAME AS BANK_NAME, AC_NO, BANK_ACCOUNT, BANK_ACCOUNT_TYPE,CURRENCY AS CURR_SYMBOL,PORTCODE AS PORTFOLIO_CODE,PORTNAME AS PORTFOLIO_NAME, AVL_BALANCE AS AVAILABLE_BALANCE FROM (`
                querySet[querySet.length] = bankBalancesQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("BankBalance: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`BankBalance: Error: ${error}`);
        }
    },
    banktransaction: async function (asondate, sessionid) {
        try {
            var querySet = []
            let bankTransactionUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V204&event=getBankingTransactionViewRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let bankTransactionQuery = await callRest(bankTransactionUrl)
            if (bankTransactionQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_bank_transaction WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_bank_transaction (ASONDATE, REVERSAL_STATUS, PRODUCT, REC_ID, BANK_ACCOUNT_ID, AC_NUMBER, BANK_NAME, POSTING_DATE, VALUE_DATE, AMOUNT1, CONVERSION_RATE, TRANS_AMOUNT, CURR_SYMBOL, COUNTERPARTY, SYSTEM_COUNTERPARTY, TYPE_OF_SETTLEMENT, ACCOUNT_DESCRIPTION, TRANSACTION_TYPE, ASSET_CLASS_NAME, TYPE_OF_TRANS, PORTFOLIO_PLAN_NAME, PORTFOLIO_CODE, FUND_CODE, FUND_NAME, EXTERNAL_PORTFOLIO_CODE, EXTERNAL_SUB_PORTFOLIO_CODE, INST_TYPE, INST_NO, REFERENCE_ID, REFERENCE_NO, REMARKS, RECON_STATUS, RECON_STATUS_BY, RECON_STATUS_ON, RECON_STATUS_REMARKS, AUTHORISATION_STATUS, AUTHORISED_BY, AUTHORISATION_DATETIME, AUTHSTATUS_REMARKS, PORTCODE, VOUCHER_NO, DEPARTMENT, BRANCH_NAME, OPERATIONS, GUID, DEAL_NO, ENTITY_NAME, TYPE, SECURITYSYMBOL, SECURITY, ADJUSTMENT, ORDER_ID, DELETED, CREATED_ON, CREATED_BY, AUTHORIZATION_DATETIME)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE, REVERSAL_STATUS, PRODUCT, REC_ID,BANKACID BANK_ACCOUNT_ID,ACNUMBER AC_NUMBER,BANKNAME BANK_NAME, POSTING_DATE, VALUE_DATE, AMOUNT1, CONVERSION_RATE, TRANS_AMOUNT,CURRENCY CURR_SYMBOL, COUNTERPARTY,SYSTEM_CPTY SYSTEM_COUNTERPARTY, TYPE_OF_SETTLEMENT,DESCRIPTION ACCOUNT_DESCRIPTION,TRANSTYPE TRANSACTION_TYPE, ASSET_CLASS_NAME, TYPE_OF_TRANS,PLAN PORTFOLIO_PLAN_NAME,PORTFOLIO PORTFOLIO_CODE, FUND_CODE, FUND_NAME, EXTERNAL_PORTCODE EXTERNAL_PORTFOLIO_CODE,EXTERNAL_SUB_PORTCODE EXTERNAL_SUB_PORTFOLIO_CODE, INST_TYPE, INST_NO,REF_RECID REFERENCE_ID,REL_REFNO REFERENCE_NO,REMARKS,RECOSTATUS RECON_STATUS,RECOSTATUSBY RECON_STATUS_BY,RECOSTATUSON RECON_STATUS_ON,RECOSTATUSREMARKS RECON_STATUS_REMARKS,AUTHSTATUS AUTHORISATION_STATUS,AUTHSTATUSBY AUTHORISED_BY,AUTHSTATUSON AUTHORISATION_DATETIME,AUTHSTATUSREMARKS AUTHSTATUS_REMARKS, PORTCODE, VOUCHER_NO, DEPARTMENT, BRANCH_NAME, OPERATIONS,GUIDS GUID,DEALNO DEAL_NO,ENTITYNAME ENTITY_NAME, TYPE, SECURITYSYMBOL,SECURITY_DESC SECURITY, ADJUSTMENT, ORDER_ID, DELETED, CREATED_ON, CREATED_BY,AUTHORIZATIONDATETIME AUTHORIZATION_DATETIME FROM (`
                querySet[querySet.length] = bankTransactionQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("BankTransaction: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`BankTransaction: Error: ${error}`);
        }
    },
    accountentries: async function (asondate, sessionid) {
        try {
            var querySet = []
            let accountDetailUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V205&event=getAcctDetailsRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let accountDetailQuery = await callRest(accountDetailUrl)
            if (accountDetailQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_acct_entries WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_acct_entries (asondate, book_name, entryid, voucher_no, entry_date, value_date, vouchername, account_id, level1accountname, level2accountname, account_name, account_type, debit, credit, dealer_ticket_no, deal_no, narration, status, debit_credit, amount, account_description, asset_class_code, portfolio_name, portfolio_code, securitysymbol, security_type, irdacategory, irdasubcategory, irda_category_code, transaction_type_code, transaction_type, custodian_accountid, gl_code, consider_for_realised_gl, gl_name, val_report_fee_payable, val_report_brokers_payable, include, custodian_srno, guid, accounting_reversed, entity_name, auto_manual, isin_no, order_id, upload_batch_no,include_for_aum,INCLUDE_FOR_FORM1)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE,bookname BOOK_NAME, ENTRYID,voucherno VOUCHER_NO,entrydate ENTRY_DATE,valuedate VALUE_DATE, VOUCHERNAME,accent.accountid ACCOUNT_ID, LEVEL1ACCOUNTNAME, LEVEL2ACCOUNTNAME,accent.accountname ACCOUNT_NAME,accent.accounttype ACCOUNT_TYPE, DEBIT, CREDIT, DEALER_TICKET_NO, DEAL_NO, NARRATION, STATUS, DEBIT_CREDIT, AMOUNT,descr ACCOUNT_DESCRIPTION, accent.ASSET_CLASS_CODE, PORTFOLIO_NAME,accent.portcode PORTFOLIO_CODE, SECURITYSYMBOL, SECURITY_TYPE, IRDACATEGORY, IRDASUBCATEGORY,catcode IRDA_CATEGORY_CODE, TRANSACTION_TYPE_CODE, TRANSACTION_TYPE,cust_accountid CUSTODIAN_ACCOUNTID,glcode GL_CODE, CONSIDER_FOR_REALISED_GL,glname GL_NAME, VAL_REPORT_FEE_PAYABLE, VAL_REPORT_BROKERS_PAYABLE, INCLUDE,cust_srno CUSTODIAN_SRNO, GUID, ACCOUNTING_REVERSED, ENTITY_NAME,a_m AUTO_MANUAL,ISIN ISIN_NO, ORDER_ID, UPLOAD_BATCH_NO,include_for_aum,INCLUDE_FOR_FORM1 FROM (`
                querySet[querySet.length] = accountDetailQuery.trim() + ") accent, ac_accountmaster acc WHERE  accent.accountid = acc.accountid; END;";
                query = querySet.join("\n");
                logger.debug(query)
                result = await updateOraDB('funds', query);
            } else {
                logger.error("AccountEntries: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`AccountEntries: Error: ${error}`);
        }
    },
    limitstatus: async function (asondate, sessionid) {
        try {
            var querySet = []
            let limitStatusUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V206&event=getLimitStatusViewRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let limitStatusQuery = await callRest(limitStatusUrl)
            if (limitStatusQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_limit_status WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_limit_status (asondate, limit_code, limit_pattern, limit_name, limit_message, limit_exposure_on, limit_value_on, soft_min, soft_max, hard_min, hard_max, available_breached, soft_min_permit, soft_max_permit, hard_min_permit, hard_max_permit, utilised_amount, utilised_max, soft_min_available, soft_max_available, hard_min_available, hard_max_available, is_online, authorisation_status, limit_type, exposure_on, value_on)"
                querySet[querySet.length] = ` select '${asondate}' asondate, limit_code, limit_pattern, limit_name,limit_msg limit_message, limit_exposure_on, limit_value_on, soft_min, soft_max, hard_min, hard_max, available_breached, soft_min_permit, soft_max_permit, hard_min_permit, hard_max_permit,util_amt utilised_amount,util_max utilised_max,soft_min_avail soft_min_available,soft_max_avail soft_max_available,hard_min_avail hard_min_available,hard_max_avail hard_max_available,isonline is_online,authorization_status authorisation_status,limit_type limit_type,exposureon exposure_on,valueon value_on FROM (`
                querySet[querySet.length] = limitStatusQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("Limit Status: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`Limit Status: Error: ${error}`);
        }
    },
    pas: async function (asondate, sessionid) {
        try {
            var querySet = []
            let pasUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V207&event=getUASRefreshRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let pasQuery = await callRest(pasUrl)
            if (pasQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_pas WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_pas (ASONDATE, PAS_DATE, PORTFOLIO, EXTERNAL_PORTFOLIO_CODE, EXTERNAL_SUB_PORTFOLIO_CODE, PORTFOLIO_PLAN_NAME, NATURE, AMOUNT, CONTRACT_AMOUNT, ACTUAL_FINANCIAL_RISK, UNITS, REMARKS, PORTFOLIO_CODE, PORTFOLIO_PLAN_CODE, PAS_RECORD_ID, CMS_REC_ID, STATUS, SOURCE, ULIP_NONULIP, REDEMPTION_UNITS)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE, PAS_DATE, PORTFOLIO,EXTERNAL_PORTCODE EXTERNAL_PORTFOLIO_CODE,EXTERNAL_SUB_PORTCODE EXTERNAL_SUB_PORTFOLIO_CODE,PLAN_NAME PORTFOLIO_PLAN_NAME, NATURE, AMOUNT, CONTRACT_AMOUNT, ACTUAL_FINANCIAL_RISK, UNITS,REMARK REMARKS,PORTCODE PORTFOLIO_CODE,PLAN_CODE PORTFOLIO_PLAN_CODE,PAS_RECID PAS_RECORD_ID, CMS_REC_ID, STATUS, SOURCE, ULIP_NONULIP,REDUNITS REDEMPTION_UNITS FROM (`
                querySet[querySet.length] = pasQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("PAS: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`PAS: Error: ${error}`);
        }
    },
    trailbalance: async function (asondate, sessionid) {
        try {
            var querySet = []
            let trailbalanceUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V208&event=getTrialBalancDetailsRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let trailbalanceQuery = await callRest(trailbalanceUrl)
            if (trailbalanceQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_trailbalance WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO his_trailbalance (ASONDATE, BOOK_NAME, LEVEL1ACCOUNTNAME, LEVEL2ACCOUNTNAME, ACCOUNT_NAME, PORTFOLIO_NAME, PORTCODE, EXTERNAL_PORTFOLIO_CODE, EXTERNAL_SUB_PORTFOLIO_CODE, OPENING_DEBIT, OPENING_CREDIT, PERIOD_DEBIT, PERIOD_CREDIT, CLOSING_DEBIT, CLOSING_CREDIT, NET_AMOUNT, GL_CODE, CUSTOM_ACCOUNTID, ACCOUNT_ID, ACCOUNT_TYPE, ACCOUNT_SUB_TYPE, ACCOUNT_DESCRIPTION, ENTITY_NAME)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE,BOOKNAME BOOK_NAME, LEVEL1ACCOUNTNAME, LEVEL2ACCOUNTNAME,ACCOUNTNAME ACCOUNT_NAME,PORTNAME PORTFOLIO_NAME, PORTCODE,EXTERNAL_PORTCODE EXTERNAL_PORTFOLIO_CODE,EXTERNAL_SUB_PORTCODE EXTERNAL_SUB_PORTFOLIO_CODE,OPENINGDEBIT OPENING_DEBIT,OPENINGCREDIT OPENING_CREDIT,PERIODDEBIT PERIOD_DEBIT,PERIODCREDIT PERIOD_CREDIT,CLOSINGDEBIT CLOSING_DEBIT,CLOSINGCREDIT CLOSING_CREDIT,NETAMT NET_AMOUNT,GLCODE GL_CODE,CUST_ACCOUNTID CUSTOM_ACCOUNTID,ACCOUNTID ACCOUNT_ID,ACCOUNTTYPE ACCOUNT_TYPE,ACCOUNTSUBTYPE ACCOUNT_SUB_TYPE,DESCR ACCOUNT_DESCRIPTION,ENTITYNAME ENTITY_NAME FROM (`
                querySet[querySet.length] = trailbalanceQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("trailbalance: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`trailbalance: Error: ${error}`);
        }
    },
    interestaccrual: async function (asondate, sessionid) {
        try {
            var querySet = []
            let interestaccrualUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V209&event=getInterestAccrualQueryRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let interestaccrualQuery = await callRest(interestaccrualUrl)
            if (interestaccrualQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_interestaccrual WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO HIS_INTERESTACCRUAL (asondate, portfolio_code, portfolio_name, securitysymbol, security, asset_class_code, asset_class_name, previpdate, accrual_date, interestaccrualpu, coupon, face_value, quantity, dailyinterest, interestaccrual, interestdays)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE,PORTCODE PORTFOLIO_CODE,PORTNAME PORTFOLIO_NAME, SECURITYSYMBOL,SECURITY_DESC SECURITY, ASSET_CLASS_CODE, ASSET_CLASS_NAME, PREVIPDATE,ACCRUALDATE ACCRUAL_DATE, INTERESTACCRUALPU, COUPON,FACEVALUE FACE_VALUE, QUANTITY, DAILYINTEREST, INTERESTACCRUAL, INTERESTDAYS FROM (`
                querySet[querySet.length] = interestaccrualQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("Interest Accrual: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`Interest Accrual: Error: ${error}`);
        }
    },
    ammortisation: async function (asondate, sessionid) {
        try {
            var querySet = []
            let ammortisationUrl = `${config.applicationIp}/Framewrk/Event.jsp?m=V2&v=V210&event=refreshSavedAmmortisationAccretionRest&_ign_um_prc_con=n&jsondata={'asondate':'${asondate}','portcode':'','securitysymbol':''}&sessionid=${sessionid}`
            let ammortisationQuery = await callRest(ammortisationUrl)
            if (ammortisationQuery.trim().length > 0) {
                querySet[querySet.length] = `BEGIN DELETE FROM his_amortisation WHERE trunc(asondate)='${asondate}'; `
                querySet[querySet.length] = "INSERT INTO HIS_AMORTISATION (ASONDATE, PORTFOLIO, ASSET_CLASS_CODE, ASSET_CLASS_NAME, SECURITY, POSTING_DATE, LASTAMMDATE, SECURITYSYMBOL, QUANTITY, WAC, BVBEFOREAMMORTISATION, AMORTIZATION_PERUNIT, TOAMMORTISEAMOUNT, AMMORTISEDBOOKVALUE, WACAFTERAMMORTISATION, AMMORTISEDPERIOD, DATEOFMATURITY, PERIODTOMATURITY, TRANSTYPE, PORTFOLIO_CODE, ORG_TOAMMORTISEAMOUNT, ENTRYID, BATCHNO, METHOD, WAY, INDEX_RATIO, ORG_WACAFTERAMMORTISATION, ORG_AMMORTISEDBOOKVALUE, COSTING_TYPE)"
                querySet[querySet.length] = ` select '${asondate}' ASONDATE,PORTNAME PORTFOLIO, ASSET_CLASS_CODE, ASSET_CLASS_NAME,SECURITY_DESC SECURITY,POSTINGDATE POSTING_DATE, LASTAMMDATE, SECURITYSYMBOL, QUANTITY, WAC, BVBEFOREAMMORTISATION, AMORTIZATION_PERUNIT, TOAMMORTISEAMOUNT, AMMORTISEDBOOKVALUE, WACAFTERAMMORTISATION, AMMORTISEDPERIOD, DATEOFMATURITY, PERIODTOMATURITY, TRANSTYPE,PORTCODE PORTFOLIO_CODE, ORG_TOAMMORTISEAMOUNT, ENTRYID, BATCHNO, METHOD, WAY, INDEX_RATIO, ORG_WACAFTERAMMORTISATION, ORG_AMMORTISEDBOOKVALUE, COSTING_TYPE FROM (`
                querySet[querySet.length] = ammortisationQuery.trim() + "); END;";
                query = querySet.join("\n");
                result = await updateOraDB('funds', query);
            } else {
                logger.error("Ammortisation: No query received from jboss");
                return null;
            }
        } catch (error) {
            throw (`Ammortisation: Error: ${error}`);
        }
    }
}