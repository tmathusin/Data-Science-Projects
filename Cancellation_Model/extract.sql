with sy_s1 AS(
SELECT offer.offerreferencenumber, 
offer.offereffectivedatetime,
date_diff('week', cast('2022-01-01' as timestamp), offer.offereffectivedatetime) + 2 as offereffectiveweek,
offer.offeracceptedprice,
offer.offerbusinesseventcode,
offer.offerpostcode,
offer.offeracceptedindicator,
case when offer.offeracceptedindicator = 'Y' then 1 else 0 end as sale,
offerfacts.offertotalprice,
offerfacts.persongoldencopyid,
offerfacts.itemgoldencopyid,
itemfacts.itemeventuserid,
-- dimcontract.contractcreateddate,
dimcontract.contractstatuscode,
-- dimcontract.contractcompanycode,
-- dimcontract.contractschemecode,
dimcontract.contractnumber,
dimcontract.contractstatusname,
dimcontract.contractenddate,
-- dimcontract.contractmovementdate,
-- dimcontract.contractoriginalstartdate,
dimcontract.contractdimensionid,
contractfacts.contracteventdatetime as movementdate,
DATE_PARSE(CONCAT(
        SUBSTR(CAST(contractfacts.contractcoverstartdatedimensionid AS VARCHAR), 1, 4), '-',
        SUBSTR(CAST(contractfacts.contractcoverstartdatedimensionid AS VARCHAR), 5, 2), '-',
        SUBSTR(CAST(contractfacts.contractcoverstartdatedimensionid AS VARCHAR), 7, 2)
    ), '%Y-%m-%d') AS planaccdate,
dimitem.itemcategoryname,
dimitem.itemsupercategorycode,
dimitem.itemgoodscategoryname,
dimitem.manufacturerbrandname,
dimitem.manufacturergroupname,
dimitem.itemmodelnumber,
dimitem.itempurchaseprice,
dimitem.itempurchasedatetime,
dimitem.itemlocationpostcode,
dimperson.personaddresspostcode
-- , planmaster.planno, 
-- planmaster.gross,
-- DATE_PARSE(CONCAT(
    --     SUBSTR(CAST(planmaster.planaccdate+19000000 AS VARCHAR), 1, 4), '-',
    --     SUBSTR(CAST(planmaster.planaccdate+19000000 AS VARCHAR), 5, 2), '-',
    --     SUBSTR(CAST(planmaster.planaccdate+19000000 AS VARCHAR), 7, 2)
    -- ), '%Y-%m-%d') AS planaccdate, 
-- DATE_PARSE(CONCAT(
    --     SUBSTR(CAST(planmaster.movementdate+19000000 AS VARCHAR), 1, 4), '-',
    --     SUBSTR(CAST(planmaster.movementdate+19000000 AS VARCHAR), 5, 2), '-',
    --     SUBSTR(CAST(planmaster.movementdate+19000000 AS VARCHAR), 7, 2)
    -- ), '%Y-%m-%d')  AS movementdate,
-- planmaster.rec_status,
-- planmaster.planstatus, 
-- planmaster.cocode,
-- planmaster.schcode

FROM (select * from "prq_datalake_11112020"."dimensions_dimoffer"  where rec_status = 1 and offereffectivedatetime >= cast('2021-01-01' as timestamp) and offereffectivedatetime <= cast('2022-09-01' as timestamp) and countrycode = 'GBR' and offerbusinesseventsourcename !='REPAIR AND PROTECT' and offerbusinesseventsourcename != 'REPLACEMENT' and recordsource in ('VEM', 'GEN') and
    offerclientname != 'ARGOS' and offerclientname != 'JOHN LEWIS' and
    offerbusinesseventcode = 'REG' and offeracceptedindicator = 'Y') offer

left join (select * from "prq_datalake_11112020"."facts_offerdetailfact" where rec_status = 1 and recordstatus = 'A' and offerpaymenttypecode = 'D') offerfacts
on offer.offerdimensionid = offerfacts.offerdimensionid

left join (select * from "prq_datalake_11112020"."dimensions_dimitem" where itemcountrycode = 'GBR' and recordstatus = 'A' and rec_status = 1) dimitem
on offerfacts.itemdimensionid = dimitem.itemdimensionid

left join (select * from "prq_datalake_11112020"."dimensions_dimperson" where personcountrycode = 'GBR' and recordstatus = 'A' and rec_status = 1) dimperson
on offerfacts.persondimensionid = dimperson.persondimensionid

left join (select * from "prq_datalake_11112020"."dimensions_dimcontract" where rec_status = 1 and contractcreateddate >= cast('2021-01-01' as timestamp) ) dimcontract
on offer.contractid = dimcontract.contractid

left join (select * from (select *,
row_number() over 
    (partition by contractdimensionid
    order by loaddatetime desc) as row_contractfacts
from "prq_datalake_11112020"."facts_contracteventfact" 
where rec_status = 1 and recordstatus = 'A' and currentflag = 'Y')
where row_contractfacts=1) contractfacts
on dimcontract.contractdimensionid = contractfacts.contractdimensionid

left join (select * from (
select *, row_number() over 
    (partition by itemdimensionid
    order by itemeventdatetime desc) as row_itemfacts
    from "prq_datalake_11112020"."facts_itemeventfact" where rec_status = 1 and recordstatus = 'A')
    where row_itemfacts = 1 ) itemfacts
on itemfacts.itemdimensionid = dimitem.itemdimensionid

-- inner join (select * from "prq_datalake_11112020"."schdayfilc_planmaster" where rec_status = 1) planmaster
-- on planmaster.planno = dimcontract.contractnumber and planmaster.cocode = dimcontract.contractcompanycode and planmaster.schcode = dimcontract.contractschemecode

)
,
sy_s3 as (
select
master.*,
pmh.PmhId, 
pmh.TotalPrice , 
pmh.PeriodOfCover,
pmh.PricingVersionDescription, 
pmh.PredictedConversionRate, 
pmh.ModelId,
perh.*

from sy_s1  master
left join (select
    PNL.PrhIdFk,
    PNL.OfferRefNum,
	PNL.perhidfk,
    PNL.OfferPricingKey
    FROM "prq_datalake_11112020"."pds_plannumberlink" as PNL
    where MetaLoadDateTime >= cast('2021-01-01' as timestamp)
    and rec_status = 1) pnl
on pnl.OfferRefNum = master.offerreferencenumber
inner join (SELECT
    PMH.PmhId, 
    PMH.TotalPrice, 
    PMH.PeriodOfCover,
    CASE WHEN MetaLoadDateTime >= cast('2021-01-01' as timestamp) and PMH.pricingversiondescription = 'TEL REG HYBRID OPT TARGET 21.56%' then 'TEL REG HYBRID (EUNICE) OPT TARGET MAX' else PMH.PricingVersionDescription end as PricingVersionDescription, 
    PMH.PredictedConversionRate, 
    PMH.ModelId,
    PMH.PrhId, 
    PMH.PriceRequestXMLAsText
    -- requested_personpostaladdrpostcode
FROM "prq_datalake_11112020"."pds_pricemodelhistory" as PMH
    where Paymenttype = 'D'
    and MetaLoadDateTime >= cast('2021-01-01' as timestamp)
    and rec_status = 1) pmh
on pmh.PrhId = pnl.PrhIdFk
inner join (SELECT
    PERH.PerhId, 
    PERH.purchasedate as requested_purchasedate, 
    PERH.purchaseprice as requested_purchaprice,
	PERH.asisfee as requested_fee,
    PERH.retentionflag as requested_retentionflag,
	PERH.planlivecount as requested_planlivecount,
	PERH.plansactivepastoneyearcount as requested_plansactivepastoneyearcount,
	PERH.plansactivepastfiveyearcount as requested_plansactivepastfiveyearcount,
	PERH.plansacceptedpastoneyearcount as requested_plansacceptedpastoneyearcount,
	PERH.plansacceptedpastfiveyearcount as requested_plansacceptedpastfiveyearcount,
	PERH.planscancelledpastoneyearcount as requested_planscancelledpastoneyearcount,
	PERH.planscancelledpastfiveyearcount as requested_planscancelledpastfiveyearcount,
	PERH.claimpastoneyearcount as requested_claimpastoneyearcount,
	PERH.claimpastthreeyearcount as requested_claimpastthreeyearcount,
	PERH.claimpastfiveyearcount as requested_claimpastfiveyearcount,
	PERH.claimamountoneyeartotal as requested_claimamountoneyeartotal,
	PERH.claimamountthreeyeartotal as requested_claimamountthreeyeartotal,
	PERH.claimamountfiveyeartotal as requested_claimamountfiveyeartotal,
	PERH.prevrtppricetype as requested_prevrtppricetype,
	PERH.buseventcode as requested_buseventcode,
	
	PERH.itemgoodscatcode as requested_itemgoodscatcode,
	PERH.clientgroupdesc as requested_clientgroupdesc,
	PERH.schemetypename as requested_schemetypename,
	PERH.clientaccountdesc as requested_clientaccountdesc,
	PERH.routetomarketname as requested_routetomarketname,
	PERH.companycode as requested_companycode,
	PERH.schemecode as requested_schemecode,
    PERH.manugteeperiodlabourmonths as requested_manugteeperiodlabourmonths,
    PERH.manugteeperiodpartsmonths as requested_manugteeperiodpartsmonths,
	PERH.itemcode as requested_itemcode,
    PERH.clientgroupcode as requested_clientgroupcode,
	PERH.clientcode as requested_clientcode,
    PERH.itemmanufbrandcode as requested_itemmanufbrandcode,
	PERH.perpostaladdrpostcode as requested_perpostaladdrpostcode
FROM "prq_datalake_11112020"."pds_pricingenginerequesthistory"  PERH
    where MetaLoadeddts >= cast('2021-01-01' as timestamp) and clientgroupdesc <> 'HEATING (OTHER)'
    and rec_status = 1) perh
on perh.PerhId = pnl.perhidfk

)
,

sy_s4 as (
select * ,
    (cast(persongoldencopyid as varchar) || replace(lower(itemcategoryname), ' ', '') || replace(upper(manufacturerbrandname), ' ', '') || 
    replace(upper(itemmodelnumber_imputed), ' ', '') ||replace(upper(itemlocationpostcode), ' ','')) as itemdiamondcopyid_imputed,
    (cast(persongoldencopyid as varchar) || replace(lower(itemcategoryname), ' ', '') || replace(upper(manufacturerbrandname), ' ', '') || 
    replace(upper(itemmodelnumber_imputed), ' ', '') ||replace(upper(itemlocationpostcode), ' ','')) as itemdiamondcopyid
from (select distinct *, 
    case when master3.itemmodelnumber = 'NA' and impute.itemmodelnumber_dup is not null then impute.itemmodelnumber_dup 
    when master3.itemmodelnumber = 'NA' and impute.itemmodelnumber_dup is null then 'XXX'
    when master3.itemmodelnumber is null and impute.itemmodelnumber_dup is not null then impute.itemmodelnumber_dup 
    when master3.itemmodelnumber is null and impute.itemmodelnumber_dup is null then 'XXX'
    else master3.itemmodelnumber end as itemmodelnumber_imputed 
from sy_s3 master3
left join (
select distinct *
from
(select persongoldencopyid as persongoldencopyid_dup, itemcategoryname as itemcategoryname_dup, manufacturerbrandname as manufacturerbrandname_dup, itemlocationpostcode as itemlocationpostcode_dup, itemmodelnumber as itemmodelnumber_dup
from sy_s3
where  (persongoldencopyid is not null) and (itemcategoryname != 'NA' or itemcategoryname is not null) and (manufacturerbrandname != 'NA' or manufacturerbrandname is not null) and (itemlocationpostcode != 'NA' or itemlocationpostcode is not null) 
group by persongoldencopyid, itemcategoryname, manufacturerbrandname, itemlocationpostcode, itemmodelnumber
)
where itemmodelnumber_dup != 'NA' and itemmodelnumber_dup is not null and manufacturerbrandname_dup != 'NA' ) impute
on impute.persongoldencopyid_dup = master3.persongoldencopyid and  impute.itemcategoryname_dup = master3.itemcategoryname and impute.manufacturerbrandname_dup = master3.manufacturerbrandname and impute.itemlocationpostcode_dup = master3.itemlocationpostcode)  master
)
, 
sy_master3 as(

select master.*,
(master.totalprice- master.requested_fee)/master.requested_fee as price_diff,
offer.offersourcereceiveddatetime 
from sy_s4 master
left join (SELECT distinct offerreferencenumber,offersourcereceiveddatetime FROM "prq_datalake_11112020"."dimensions_dimoffer" 
where rec_status = 1 and offereffectivedatetime >= cast('2021-01-01' as timestamp) and countrycode = 'GBR' and offerbusinesseventsourcename !='REPAIR AND PROTECT' and offerbusinesseventsourcename != 'REPLACEMENT' and offerchannelname <> 'CAMALOT' and
    offerclientname != 'ARGOS' and offerclientname != 'JOHN LEWIS') offer
on master.offerreferencenumber = offer.offerreferencenumber
WHERE master.offereffectivedatetime >= cast('2021-01-01' as timestamp)
)

select 
-- count (*), count(requested_purchasedate), count(requested_perpostaladdrpostcode), min(movementdate), max(movementdate), min(planaccdate), max(planaccdate)
*
     ,case when price_diff +1 > 0.699 and price_diff+1 <= 0.8
     then '[0.699, 0.8]'
     when price_diff +1 > 0.8 and price_diff +1 <= 0.9 
     then '[0.8, 0.9]' 
     when price_diff+1 > 0.9 and price_diff+1 <= 0.99 
     then '[0.9, 0.99]' 
     when price_diff+1 > 0.99 and price_diff+1 <= 1 
     then '[0.99, 1.0]' 
     when price_diff+1 > 1 and price_diff+1 <= 1.1 
     then '[1.0, 1.1]' 
     when price_diff+1 > 1.1 and price_diff+1 <= 1.2 
     then '[1.1, 1.2]' 
     when price_diff+1 > 1.2 and price_diff+1 <= 1.34 
     then '[1.2, 1.34]' else null end as price_band

    from sy_master3
    where requested_fee is not null

