module.exports = emitter => {
  emitter.on("dealprocess", async msg => {
    logger.info("DealProcess")
    var updateOraDB = funclib['updateOraDB']
    var notify = funclib['notify']
    result = await updateOraDB('funds', `BEGIN UPDATE OPD_summary_details SET status_authorise = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised')); UPDATE OPD_summary_details SET status_confirm = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed')); UPDATE OPD_summary_details SET authorise_eq = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class = 'EQUITY' ), authorise_mf = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class = 'MUTUAL FUND'), authorise_bonds = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class = 'BONDS'), authorise_gilts = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class = 'GILTS'), authorise_fd = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class = 'DEPOSITS'), authorise_oth = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Authorised') AND asset_class NOT IN ('EQUITY','MUTUAL FUND','BONDS','GILTS','DEPOSITS')); UPDATE OPD_summary_details SET confirm_eq = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class = 'EQUITY' ), confirm_mf = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class = 'MUTUAL FUND'),confirm_bonds = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class = 'BONDS'),confirm_gilts = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class = 'GILTS'),confirm_fd = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class = 'DEPOSITS'),confirm_oth = (SELECT Count(*) FROM trade_Data WHERE deal_status IN ('Not Confirmed') AND asset_class NOT IN ('EQUITY','MUTUAL FUND','BONDS','GILTS','DEPOSITS')); END;`);
    if (result.status)
      notify("deal","dealprocess")
    else
      logger.warn("DealProcess: update query failed")
  });

  emitter.on("settleprocess", async msg => {
    logger.info("settleprocess")
    var updateOraDB = funclib['updateOraDB']
    var notify = funclib['notify']
    result = await updateOraDB('funds', `BEGIN UPDATE OPD_summary_details SET status_settlement = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed')); UPDATE OPD_summary_details SET status_sett_auth = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle')); UPDATE OPD_summary_details SET settle_eq = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class = 'EQUITY' ), settle_mf = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class = 'MUTUAL FUND'), settle_bonds = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class = 'BONDS'), settle_gilts = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class = 'GILTS'), settle_fd = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class = 'DEPOSITS'), settle_oth = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Confirmed') AND asset_class NOT IN ('EQUITY','MUTUAL FUND','BONDS','GILTS','DEPOSITS')); UPDATE OPD_summary_details SET settle_auth_eq = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class = 'EQUITY' ), settle_auth_mf = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class = 'MUTUAL FUND'), settle_auth_bonds = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class = 'BONDS'), settle_auth_gilt = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class = 'GILTS'), settle_auth_fd = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class = 'DEPOSITS'), settle_auth_oth = (SELECT Count(*) FROM opd_settle_data WHERE deal_status IN ('Settle') AND asset_class NOT IN ('EQUITY','MUTUAL FUND','BONDS','GILTS','DEPOSITS')); END;`);
    
    if (result.status)
      notify("settle","settleprocess")
    else
      logger.warn("settleprocess: update query failed")
  });

  emitter.on("caprocess", async msg => {
    logger.info("caprocess")
    var updateOraDB = funclib['updateOraDB']
    var notify = funclib['notify']
    result = await updateOraDB('funds', `BEGIN UPDATE OPD_summary_details SET corpor_event_pending = (SELECT Count(*) FROM OPD_corpevent_details ); UPDATE OPD_summary_details SET dividends = (SELECT Count(*) FROM OPD_corpevent_details WHERE event_type IN ('DIVIDEND')), bonus = (SELECT Count(*) FROM OPD_corpevent_details WHERE event_type IN ('BONUS')),interest = (SELECT Count(*) FROM OPD_corpevent_details WHERE event_type IN ('INTEREST')),redemptions = (SELECT Count(*) FROM OPD_corpevent_details WHERE event_type IN ('REDEMPTION')),others = (SELECT Count(*) FROM OPD_corpevent_details WHERE event_type NOT IN ('REDEMPTION','DIVIDEND','BONUS','INTEREST','REDEMPTION')); END;`);
    
    if (result.status)
      notify("corpor","caprocess")
    else
      logger.warn("caprocess: update query failed")
  });

  emitter.on("uploadprocess", async msg => {
    logger.info("uploadprocess")
    var updateOraDB = funclib['updateOraDB']
    var notify = funclib['notify']
    result = await updateOraDB('funds', `BEGIN UPDATE OPD_summary_details SET upload_pending = (SELECT Count(*) FROM OPD_Upload_details where next_action IN ('process')); UPDATE OPD_summary_details SET upload_confirm = (SELECT Count(*) FROM OPD_Upload_details WHERE next_action IN ('view')); END;`);
    if (result.status)
      notify("upload","uploadprocess")
    else
      logger.warn("uploadprocess: update query failed")
  });

};


testMsg = {
  "eventname": "dealconfirmation",
  "data": {
    "message": "testing sendmail event"
  }
}