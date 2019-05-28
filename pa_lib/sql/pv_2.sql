with fl_pv as (
    select  /*+ parallel(4) */
            fl.fk_flaeche   flaeche,
            fl.fk_pv_pos    pv_pos_bridge,
            pv.pv_position_id pv_pos,
            fl.umsatz_brutto      * fl_pv.gewicht brutto,  -- Aufteilen vom Flächenumsatz auf mehrere PV-Pos
            fl.umsatz_netto_netto * fl_pv.gewicht netto,   -- Aufteilen vom Flächenumsatz auf mehrere PV-Pos
            fl.RESERVATION_ISO_KW res_Jahr_Kw,
            fl.aushangtag_iso_kw aus_Jahr_Kw,
            pv.pv_nr,
            pv.pv_titel pv_titel,
            pv.partner_nr partner_nr,
            pv.partner partner
       from vkub_flaechen_umsatz_aw_v fl
           ,at_d_pv_pos_br_t fl_pv
           ,vkub_d_partner_v pv
      where fl.fk_pv_pos = fl_pv.pvpos_bridge_id
        and pv.pv_position_id = fl_pv.pv_position_id
        and pv_nr != -1
        -- APC cubes go back 5 years only (for Aushang, reservations may be earlier)
        and fl.RESERVATION_ISO_KW >= extract(year from sysdate) - 5 || '01'
        and fl.aushangtag_iso_kw  >= extract(year from sysdate) - 5 || '01'
),
res as (
    select sum(brutto) brutto, 
           sum(netto) netto, 
           res_Jahr_Kw Jahr_Kw,
           pv_nr,
           pv_nr || '|' || Res_Jahr_Kw pv_nr_kw,
           min(pv_titel) pv_titel,
           min(partner_nr) partner_nr,
           min(partner) partner
      from fl_pv
     group by pv_nr, Res_Jahr_Kw
    having sum(brutto) > 0 or sum(netto) > 0 
),
aus as (
    select sum(brutto) brutto, 
           sum(netto) netto, 
           aus_Jahr_Kw Jahr_Kw,
           pv_nr,
           pv_nr || '|' || Aus_Jahr_Kw pv_nr_kw,
           min(pv_titel) pv_titel,
           min(partner_nr) partner_nr,
           min(partner) partner
      from fl_pv
     group by pv_nr, Aus_Jahr_Kw
    having sum(brutto) > 0 or sum(netto) > 0 
)
select coalesce(res.pv_nr, aus.pv_nr) pv_nr,
       coalesce(res.Jahr_Kw, aus.Jahr_Kw) Jahr_Kw,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 1, 4) Jahr,
       substr(coalesce(res.Jahr_Kw, aus.Jahr_Kw), 5, 6) Kw,
       coalesce(res.brutto, 0) res_brutto,
       coalesce(res.netto, 0)  res_netto,
       coalesce(aus.brutto, 0) aus_brutto,
       coalesce(aus.netto, 0)  aus_netto,
       coalesce(res.pv_titel, aus.pv_titel) pv_titel,
       coalesce(res.partner_nr, aus.partner_nr) partner_nr,
       coalesce(res.partner, aus.partner) partner
  from res full outer join aus
    on res.pv_nr_kw = aus.pv_nr_kw