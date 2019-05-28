with res as (
    select sum(fl.umsatz_brutto) brutto, 
           sum(fl.umsatz_netto_netto) netto, 
           fl.RESERVATION_ISO_KW Jahr_Kw,
           pv.pv_nr,
           pv.pv_nr || '|' || fl.RESERVATION_ISO_KW pv_nr_kw,
           min(pv.pv_titel) pv_titel,
           min(pv.partner_nr) partner_nr,
           min(pv.partner) partner
      from vkub_flaechen_umsatz_aw_v fl
          ,vkub_d_partner_v pv
     where fl.fk_pv_pos = pv.pv_position_id
     group by pv.pv_nr, fl.RESERVATION_ISO_KW
    having sum(fl.umsatz_brutto) > 0 or sum(fl.umsatz_netto_netto) > 0         
), 
aus as (
    select sum(fl.umsatz_brutto) brutto, 
           sum(fl.umsatz_netto_netto) netto, 
           fl.AUSHANGTAG_ISO_KW Jahr_Kw,
           pv.pv_nr,
           pv.pv_nr || '|' || fl.AUSHANGTAG_ISO_KW pv_nr_kw,
           min(pv.pv_titel) pv_titel,
           min(pv.partner_nr) partner_nr,
           min(pv.partner) partner
      from vkub_flaechen_umsatz_aw_v fl
          ,vkub_d_partner_v pv
     where fl.fk_pv_pos = pv.pv_position_id
     group by pv.pv_nr, fl.AUSHANGTAG_ISO_KW
    having sum(fl.umsatz_brutto) > 0 or sum(fl.umsatz_netto_netto) > 0   
)
select coalesce(res.pv_nr, aus.pv_nr) pv_nr,
       coalesce(res.Jahr_Kw, aus.Jahr_Kw) Jahr_Kw,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 1, 4) Jahr,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 5, 6) Kw,
       res.brutto res_brutto,
       res.netto  res_netto,
       aus.brutto aus_brutto,
       aus.netto  aus_netto,
       coalesce(res.pv_titel, aus.pv_titel) pv_titel,
       coalesce(res.partner_nr, aus.partner_nr) partner_nr,
       coalesce(res.partner, aus.partner) partner
   from res full outer join aus
     on res.pv_nr_kw = aus.pv_nr_kw
     -- APC cubes go back 5 years only (for Aushang, reservations may be earlier)
  where res.jahr_kw >= extract(year from sysdate) - 5 || '01'
    and aus.jahr_kw >= extract(year from sysdate) - 5 || '01'
    and res.pv_nr != -1