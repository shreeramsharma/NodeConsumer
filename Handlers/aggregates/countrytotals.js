module.exports = emitter => {
  emitter.on("countrytotals", msg => {
      // fire query to total country level
      var updateOraDB =funclib['updateOraDB']
      return updateOraDB('vizhome',`BEGIN delete from countrycount ; insert into countrycount select Nationality, count(*) from newplayers  group by Nationality; END;`);
  });
  emitter.on("notcountrytotals", msg => {
    // fire query to total country level
    var updateOraDB =funclib['updateOraDB']
    return updateOraDB('vizhome',`BEGIN delete from countrycount; END;`);
});
};
