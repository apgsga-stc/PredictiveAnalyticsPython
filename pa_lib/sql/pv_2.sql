with flaeche_pv as (
    select  /*+ parallel(4) */
            fl.fk_pv_pos    pv_pos_bridge,
            pv.pv_position_id pv_pos,
            fl.umsatz_brutto brutto, 
            fl.umsatz_netto_netto netto_netto, 
            fl.RESERVATION_ISO_KW Res_Jahr_Kw,
            fl.aushangtag_iso_kw Aus_Jahr_Kw,
            pv.pv_nr,
            pv.pv_titel pv_titel,
            pv.partner_nr partner_nr,
            pv.partner partner
       from vkub_flaechen_umsatz_aw_v fl
           ,vkub_d_partner_v pv
      where fl.fk_pv_pos = pv.pv_position_id
        and pv_nr != -1
        -- APC cubes go back 5 years for Aushang, reservations may be earlier ==> cut them off
        and fl.RESERVATION_ISO_KW >= extract(year from sysdate) - 5 || '01'
),
res as (
    select sum(brutto) brutto, 
           sum(netto_netto) netto_netto, 
           Res_Jahr_Kw Jahr_Kw,
           pv_nr,
           pv_nr || '|' || Res_Jahr_Kw pv_nr_kw,
           min(pv_titel) pv_titel,
           min(partner_nr) partner_nr,
           min(partner) partner
      from flaeche_pv
     group by pv_nr, Res_Jahr_Kw
    having sum(brutto) > 0 or sum(netto_netto) > 0 
),
aus as (
    select sum(brutto) brutto, 
           sum(netto_netto) netto_netto, 
           Aus_Jahr_Kw Jahr_Kw,
           pv_nr,
           pv_nr || '|' || Aus_Jahr_Kw pv_nr_kw,
           min(pv_titel) pv_titel,
           min(partner_nr) partner_nr,
           min(partner) partner
      from flaeche_pv
     group by pv_nr, Aus_Jahr_Kw
    having sum(brutto) > 0 or sum(netto_netto) > 0 
)
select coalesce(res.pv_nr, aus.pv_nr) pv_nr,
       coalesce(res.Jahr_Kw, aus.Jahr_Kw) Jahr_Kw,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 1, 4) Jahr,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 5, 6) Kw,
       nvl(res.brutto, 0) res_brutto,
       nvl(res.netto_netto, 0)  res_netto_netto,
       nvl(aus.brutto, 0) aus_brutto,
       nvl(aus.netto_netto, 0)  aus_netto_netto,
       coalesce(res.pv_titel, aus.pv_titel) pv_titel,
       coalesce(res.partner_nr, aus.partner_nr) partner_nr,
       coalesce(res.partner, aus.partner) partner
  from res full outer join aus
    on res.pv_nr_kw = aus.pv_nr_kw