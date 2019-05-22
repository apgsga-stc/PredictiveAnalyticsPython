WITH endkunden AS 
( 
SELECT /*+ materialize */ ekx.subj_oid, ekx.geschaeftspartner_nr, ekx.kombi_name, ekx.abc_kunden, 
        ekx.post_plz,  ekx.post_ort, ekx.post_postland, ekx.hbapg_kurzz, ekx.brc_id, ekx.brc_sek_id
    FROM subjekte_denorm_tot_v ekx 
    WHERE (SELECT max(su.gueltig_von) 
            FROM subjekte_denorm_tot_v su 
            WHERE su.subj_oid = ekx.subj_oid) BETWEEN ekx.gueltig_von  AND ekx.gueltig_bis  
),
--------------------------------------
-- Flächen-Eigenschaften (Agglo/WG/PF)
--------------------------------------
flzo_daten AS 
( 
SELECT /*+ PARALLEL (fz 32) */ kv.endkunde_subj_oid,  
        util.util_pa.strl40002str(CAST(COLLECT(DISTINCT ga.geb_gebiet_code) AS util.str_l))  agglo,  
        util.util_pa.strl40002str(CAST(COLLECT(DISTINCT gw.geb_gebiet_code) AS util.str_l))  wg,
        util.util_pa.strl40002str(CAST(COLLECT(DISTINCT fl.pf_bez)          AS util.str_l))  pf,
        util.util_pa.strl40002str(CAST(COLLECT(DISTINCT to_char(fl.pf_id))  AS util.str_l))  pf_id
   FROM kunde_vertraege_v     kv
   JOIN auftraege_v           ag ON kv.kdvt_id  = ag.kdvt_id
   JOIN auftrag_positionen_v  ap ON ag.ag_id    = ap.ag_id
   JOIN agps_fl_zuordnungen_v fz ON ap.agps_id  = fz.agps_id
   JOIN flaechen_denorm_tot_v fl ON fz.fl_oid   = fl.fl_oid             AND TRUNC(fz.aushang_beginn) BETWEEN fl.gueltig_von AND fl.gueltig_bis
   LEFT JOIN gebiete_blatt_tot_v  ga ON fl.gbot_oid = ga.blatt_gbot_oid AND TRUNC(SYSDATE)           BETWEEN ga.gueltig_von AND ga.gueltig_bis AND ga.geb_gbat_id = 8  -- Agglo
   LEFT JOIN gebiete_blatt_tot_v  gw ON fl.gbot_oid = gw.blatt_gbot_oid AND TRUNC(SYSDATE)           BETWEEN gw.gueltig_von AND gw.gueltig_bis AND gw.geb_gbat_id = 7  -- WG
  WHERE ap.aushang_beginn > to_date('29.12.2008','dd.mm.yyyy')
  GROUP BY kv.endkunde_subj_oid
)
/*
================================================
Main Select: Join Endkunden- mit Buchungsdaten
================================================
*/
SELECT /*+ Predictive Analytics: Read Buchungen data (runs for ca. 15 min) */
    ek.geschaeftspartner_nr                        ENDKUNDE_NR,                                             
    ek.kombi_name                                  ENDKUNDE,                        
    ek.abc_kunden                                  EK_ABC,
    ek.post_plz                                    EK_PLZ,
    ek.post_ort                                    EK_ORT,
    ek.post_postland                               EK_LAND, 
    ek.hbapg_kurzz                                 EK_HB_APG_KURZZ,
    DECODE(eka.subj_oid,NULL,0,1)                  EK_AKTIV,
    
    ebg.branche_id                                 ENDKUNDE_BRANCHENGRUPPE_ID,
    ebg.bezeichnung                                ENDKUNDE_BRANCHENGRUPPE,                                      
    ebk.branche_id                                 ENDKUNDE_BRANCHENKAT_ID,
    ebk.bezeichnung                                ENDKUNDE_BRANCHENKAT,                      
                    
    enbg.branche_id                                ENDKUNDE_NBRANCHENGRUPPE_ID,
    enbg.bezeichnung                               ENDKUNDE_NBRANCHENGRUPPE,                                      
    enbk.branche_id                                ENDKUNDE_NBRANCHENKAT_ID,
    enbk.bezeichnung                               ENDKUNDE_NBRANCHENKAT, 
    
    kamp_daten.*,  
            
    fz_daten.pf, 
    fz_daten.agglo, 
    fz_daten.wg 
FROM 
(
    --------------------------------------
    -- Kampagnen-/Buchungs-Daten
    --------------------------------------
    SELECT /*+ PARALLEL (ap 32) */ 
        substr(sg.bezeichnung,1,32)                         SEGMENT,
        endkunde_subj_oid,
                    
        au.geschaeftspartner_nr                             ATGEB_NR,                                             
        au.kombi_name                                       AUFTRAGGEBER, 
        wa.geschaeftspartner_nr                             AGENTUR_NR,                           
        wa.kombi_name                                       AGENTUR, 

        kv.kampagne_nummer                                  KV_NR, 
        kv.kdvt_type                                        KV_TYP,
        op.opoe_bezeichnung                                 VERKAUFS_OE,
        vb.kurzzeichen                                      VERKAUFSBERATER,  
        hb.kurzzeichen                                      HAUPTBETREUER,   

        kv.status                                           KAMPAGNEN_STATUS,
        ROUND(COALESCE(pk.netto_preis,pk2.netto_preis,0))   KAMPAGNE_NETTO_PREIS,
                
        TRUNC(kv.erfasst_am)                                                                                        KAMPAGNE_ERFASSUNGSDATUM,
        TO_CHAR(kv.erfasst_am, 'iyyy')                                                                              KAMP_ERF_JAHR,
        CASE WHEN TO_CHAR(kv.erfasst_am, 'iw') = 53 THEN '52' ELSE TO_CHAR(kv.erfasst_am, 'iw') END                 KAMP_ERF_KW, 
        
        km.min_aushang_beginn                                                                                       KAMPAGNE_BEGINN,                          
        TO_CHAR(km.min_aushang_beginn, 'iyyy')                                                                      KAMP_BEGINN_JAHR,
        CASE WHEN TO_CHAR(km.min_aushang_beginn, 'iw') = 53 THEN '52' ELSE TO_CHAR(km.min_aushang_beginn, 'iw') END KAMP_BEGINN_KW,

        abg.branche_id                                      AUFTRAG_BRANCHENGRUPPE_ID, 
        abg.bezeichnung                                     AUFTRAG_BRANCHENGRUPPE,                                      
        abk.branche_id                                      AUFTRAG_BRANCHENKAT_ID,
        abk.bezeichnung                                     AUFTRAG_BRANCHENKAT,  

        anbg.branche_id                                     AUFTRAG_NBRANCHENGRUPPE_ID,
        anbg.bezeichnung                                    AUFTRAG_NBRANCHENGRUPPE,                                      
        anbk.branche_id                                     AUFTRAG_NBRANCHENKAT_ID,
        anbk.bezeichnung                                    AUFTRAG_NBRANCHENKAT, 
                
        substr(ay.bedeutung,1,32)                           AUFTRAGSART,                                        
        ap.auftragspositionsnummer                          AGPS_NR,  
        ap.agps_id,    
                            
        TRUNC(ap.reservation_datum)                         RES_DAT, 
        TO_CHAR(ap.reservation_datum, 'iyyy')               RES_JAHR,
        CASE WHEN TO_CHAR(ap.reservation_datum, 'iw') = 53 THEN '52' 
             ELSE TO_CHAR(ap.reservation_datum, 'iw') END   RES_KW,                              
        TRUNC(ap.aushang_beginn)                            AUSH_vON,
        TO_CHAR(ap.aushang_beginn, 'iyyy')                  AUSH_JAHR,
        CASE WHEN TO_CHAR(ap.aushang_beginn, 'iw') = 53 THEN '52' 
             ELSE TO_CHAR(ap.aushang_beginn, 'iw') END      AUSH_KW,  
        ap.aushang_dauer                                    DAUER  , 
        
        TRUNC(ap.annullation_datum)                         ANNULLATION_DATUM,
        TRUNC(ap.bestaetigung_datum)                        BESTAETIGUNG_DATUM,  
        
        CASE WHEN TRUNC(ap.aushang_beginn)     -  TRUNC(ap.reservation_datum) > 0 THEN TRUNC(ap.aushang_beginn)     -  TRUNC(ap.reservation_datum) ELSE 0 END  AH_RESDAT_DIFF,
        CASE WHEN TRUNC(ap.aushang_beginn)     -  TRUNC(kv.erfasst_am)        > 0 THEN TRUNC(ap.aushang_beginn)     -  TRUNC(kv.erfasst_am)        ELSE 0 END  AH_KAMPDAT_DIFF,  
        CASE WHEN TRUNC(km.min_aushang_beginn) -  TRUNC(kv.erfasst_am)        > 0 THEN TRUNC(km.min_aushang_beginn) -  TRUNC(kv.erfasst_am)        ELSE 0 END  MINAH_KAMPDAT_DIFF,                    
                                                                
        DECODE(ap.agps_type, 'AGPSLG','Ja','Nein')          VERTRAG, 
        CASE WHEN ap.aushang_dauer >= 62 THEN 1 ELSE 0 END  LONGRUNNER,             
                            
        ROUND(COALESCE(pr.brutto_preis,pr2.brutto_preis,0)) BRUTTO, 
        ROUND(COALESCE(pr.netto_preis, pr2.netto_preis,0))  NETTO
                
    FROM auftrag_positionen_v     ap                                        
    JOIN auftraege_v              ag  ON ap.ag_id      = ag.ag_id                                         
    JOIN kunde_vertraege_v        kv  ON ag.kdvt_id    = kv.kdvt_id  
    LEFT JOIN kampagne_minmax_v        km  ON kv.kdvt_id    = km.kdvt_id         
    JOIN vk.dachkampagnen_mndt_gfe_v  sg ON kv.kdvt_id = sg.kdvt_id     
        
    LEFT JOIN pe.vk_preis_agps_perm_v  pr  ON ap.agps_id = pr.agps_id   
    LEFT JOIN pe.vk_preis_kdvt_perm_v  pk  ON kv.kdvt_id = pk.kdvt_id    
            
    LEFT JOIN pe.pe_preis_arch_agps_v  pr2 ON ap.agps_id = pr2.agps_id  
    LEFT JOIN pe.pe_preis_arch_kdvt_v  pk2 ON kv.kdvt_id = pk2.kdvt_id 
                                                                                                                    
    JOIN subjekte_denorm_tot_v      au  ON kv.atgeb_id              = au.atgeb_id  AND TRUNC(kv.atgeb_date)         BETWEEN au.gueltig_von  AND au.gueltig_bis    
    JOIN subjekte_denorm_tot_v      op  ON kv.vkoe_oid              = op.vkoe_oid  AND TRUNC(kv.vkoe_date)          BETWEEN op.gueltig_von  AND op.gueltig_bis
    JOIN subjekte_denorm_tot_v      VB  ON kv.vkber_oid             = vb.vkber_oid AND TRUNC(kv.vkber_date)         BETWEEN vb.gueltig_von  AND vb.gueltig_bis
    JOIN subjekte_denorm_tot_v      hb  ON kv.vkber_hbe_oid         = hb.vkber_oid AND TRUNC(kv.vkber_hbe_date)     BETWEEN hb.gueltig_von  AND hb.gueltig_bis
    LEFT JOIN subjekte_denorm_tot_v wa  ON kv.werbeagentur_subj_oid = wa.subj_oid  AND TRUNC(kv.werbe_agentur_date) BETWEEN wa.gueltig_von  AND wa.gueltig_bis 
                                            
    JOIN branchen_bs_v            abb ON ag.brc_id                      = abb.brc_id
    JOIN branchen_bs_v            abg ON abb.brc_id_ist_untergruppe_von = abg.brc_id
    JOIN branchen_bs_v            abk ON abg.brc_id_ist_untergruppe_von = abk.brc_id
                
    LEFT JOIN (SELECT anb2.ag_id, MIN(anb2.brc_id) brc_id FROM ag_brc_sekundaer_v anb2 GROUP BY anb2.ag_id) anb ON ag.ag_id = anb.ag_id 
    LEFT JOIN branchen_bs_v            anbb ON anb.brc_id                      = anbb.brc_id
    LEFT JOIN branchen_bs_v            anbg ON DECODE(anbb.branche_art, 1, anbb.brc_id_ist_untergruppe_von, anbb.brc_id) =  anbg.brc_id
    LEFT JOIN branchen_bs_v            anbk ON anbg.brc_id_ist_untergruppe_von = anbk.brc_id     
                                        
    JOIN edom.agps_status_bs_v          ax ON ap.status  = ax.code_wert     
    JOIN edom.kunde_vertrag_status_bs_v kx ON kv.status  = kx.code_wert          
    JOIN edom.auftrag_art_bs_v          ay ON ag.agat_id = ay.code_wert                  
                                    
    WHERE ap.aushang_beginn    >= to_date('29.12.2008','dd.mm.yyyy')             -- to_date('26.12.2011','dd.mm.yyyy') 
      AND ap.aushang_beginn    <  SYSDATE + (3 * 365)                            -- Fehlerfassungen ausschliessen (mehr als 3 Jahre in Zukunft)
      AND ap.reservation_datum <  TRUNC(SYSDATE-1)                               -- aktuellen Tag ausschliessen
      AND ap.reservation_datum >  to_date('01.01.2007','dd.mm.yyyy')             -- Inkonsistenzen ausschliessen
) kamp_daten
    JOIN endkunden                ek  ON kamp_daten.endkunde_subj_oid = ek.subj_oid 
LEFT JOIN subjekte_denorm_tot_v    eka ON kamp_daten.endkunde_subj_oid = eka.subj_oid AND TRUNC(SYSDATE) BETWEEN eka.gueltig_von AND eka.gueltig_bis 

LEFT JOIN branchen_bs_v            ebb ON ek.brc_id = ebb.brc_id
LEFT JOIN branchen_bs_v            ebg ON DECODE(ebb.branche_art, 1, ebb.brc_id_ist_untergruppe_von, ebb.brc_id) = ebg.brc_id         -- 1 = Basisbranche, oder sonst wurde bereits die Branchengruppe erfasst
LEFT JOIN branchen_bs_v            ebk ON ebg.brc_id_ist_untergruppe_von   = ebk.brc_id

LEFT JOIN branchen_bs_v            enbb ON ek.brc_sek_id = enbb.brc_id
LEFT JOIN branchen_bs_v            enbg ON DECODE(enbb.branche_art, 1, enbb.brc_id_ist_untergruppe_von, enbb.brc_id) = enbg.brc_id      
LEFT JOIN branchen_bs_v            enbk ON enbg.brc_id_ist_untergruppe_von = enbk.brc_id  
    
LEFT JOIN flzo_daten fz_daten   ON kamp_daten.endkunde_subj_oid = fz_daten.endkunde_subj_oid
