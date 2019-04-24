#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queries for PA data exports

@author: kpf
"""

QUERY = dict(
            plz = '''
                WITH plz_vkgeb AS (
                    SELECT '1001' plz, 'V-W03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1002' plz, 'V-W03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1014' plz, 'V-W02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '1211' plz, 'V-W01' verkaufs_gebiets_code FROM dual UNION
                    SELECT '1251' plz, 'V-W02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '1401' plz, 'V-W02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1701' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1705' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1709' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '1951' plz, 'V-W05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '2001' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '2002' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '2006' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '2301' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '2302' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '2304' plz, 'V-W04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '2500' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION
                    SELECT '2501' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3000' plz, 'V-M04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '3001' plz, 'V-M04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3003' plz, 'V-M04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3030' plz, 'V-M04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3050' plz, 'V-M04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3321' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION
                    SELECT '3601' plz, 'V-M05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3602' plz, 'V-M05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '3607' plz, 'V-M05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4002' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4005' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4009' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4010' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4011' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4012' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4013' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4016' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4018' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4019' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4020' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4023' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4031' plz, 'V-M01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4501' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4502' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4503' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4601' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4603' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4901' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '4902' plz, 'V-M03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '5001' plz, 'V-M06' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '5201' plz, 'V-M06' verkaufs_gebiets_code FROM dual UNION
                    SELECT '5232' plz, 'V-M06' verkaufs_gebiets_code FROM dual UNION
                    SELECT '5401' plz, 'V-M06' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '5402' plz, 'V-M06' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6000' plz, 'V-M07' verkaufs_gebiets_code FROM dual UNION
                    SELECT '6002' plz, 'V-M07' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6021' plz, 'V-M07' verkaufs_gebiets_code FROM dual UNION
                    SELECT '6061' plz, 'V-M08' verkaufs_gebiets_code FROM dual UNION
                    SELECT '6301' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6302' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6304' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6341' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6405' plz, 'V-M08' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6431' plz, 'V-M08' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6501' plz, 'V-S01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6601' plz, 'V-S01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6604' plz, 'V-S01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6836' plz, 'V-S02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '6901' plz, 'V-S02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6902' plz, 'V-S02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6903' plz, 'V-S02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '6904' plz, 'V-S02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '7001' plz, 'V-O05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '7002' plz, 'V-O05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '7004' plz, 'V-O05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '7006' plz, 'V-O05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '7007' plz, 'V-O05' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8010' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION
                    SELECT '8021' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8022' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8023' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8024' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8026' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8027' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8031' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8034' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8036' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8040' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8042' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8058' plz, 'V-O01' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8060' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '8066' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8070' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8080' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '8081' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8085' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8087' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8090' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8091' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8092' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8093' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8098' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8201' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8238' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8301' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '8401' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8402' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8403' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8410' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8501' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8502' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8503' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8510' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '8612' plz, 'V-O02' verkaufs_gebiets_code FROM dual UNION
                    SELECT '8622' plz, 'V-O03' verkaufs_gebiets_code FROM dual UNION
                    SELECT '9001' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '9004' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '9006' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '9007' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION 
                    SELECT '9013' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION  
                    SELECT '9201' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '9471' plz, 'V-O04' verkaufs_gebiets_code FROM dual UNION
                    SELECT '9501' plz, 'V-O04' verkaufs_gebiets_code FROM dual )
                SELECT /*+ Predictive Analytics: Read PLZ data */
                            pg.plz, NULL fraktion, NULL ort, pg.verkaufs_gebiets_code, gp.ma_kurzzeichen vb_vkgeb 
                    FROM plz_vkgeb               pg
                    JOIN gebiete_tot_v           gb ON pg.verkaufs_gebiets_code = gb.gebiet_code AND TRUNC(SYSDATE) BETWEEN gb.gueltig_von AND gb.gueltig_bis
                    JOIN fm.geb_ma_zuordnungen_v gm ON gb.gbot_oid = gm.gbot_oid AND gm.geb_zustaendigkeit = 1
                    JOIN geschpartner_all_tot_v  gp ON gm.subt_oid = gp.subt_oid AND TRUNC(SYSDATE) BETWEEN gp.gueltig_von AND gp.gueltig_bis
                UNION
                    SELECT DISTINCT to_char(plz.plz) plz,
                            plz.bezeichnung           fraktion,
                            ort.geb_bezeichnung       ort,
                            geb.geb_gebiet_code       verkaufs_gebiets_code,
                            gp.ma_kurzzeichen         vb_vkgeb 
                    FROM plz_blattgeb_mapping_v plz 
                LEFT JOIN gebiete_blatt_tot_v       geb ON plz.gbot_oid = geb.blatt_gbot_oid AND TRUNC(SYSDATE) BETWEEN geb.gueltig_von AND geb.gueltig_bis AND geb.geb_gbat_id = 751
                LEFT JOIN fm.geb_ma_zuordnungen_v   gm  ON geb.geb_gbot_oid = gm.gbot_oid AND gm.geb_zustaendigkeit = 1
                LEFT JOIN geschpartner_all_bs_v     gp  ON gm.subt_oid = gp.subt_oid 
                LEFT JOIN gebiete_blatt_tot_v       ort   ON plz.gbot_oid = ort.blatt_gbot_oid AND TRUNC(SYSDATE) BETWEEN ort.gueltig_von AND ort.gueltig_bis AND ort.geb_gbat_id = 4   
                    WHERE TRUNC(SYSDATE) BETWEEN plz.gueltig_von AND plz.gueltig_bis
            ''',
    
            ####################################################################################################           
            crm = '''
                SELECT /*+ Predictive Analytics: Read CRM data */
                        cm.display_name                                          betreff,
                        (select bsiutl_ucText(chn.uc_uid, 246)
                            from bsi_uc chn
                            where cm.channel_uid = chn.uc_uid)                   kanal,
                        TRUNC(cm.evt_start)                                      starttermin,
                        (select bsiutl_ucText(qu.uc_uid, 246)
                            from bsi_uc qu
                            where cm.type_uid    = qu.uc_uid)                    quelle,
                        (select display_name
                            from bsi_person p
                            where cm.responsible_user_nr = p.person_nr)          verantwortlich,
                        (select upper(du.username)  
                            from bsi_directory_user du
                            where cm.responsible_user_nr = du.directory_user_nr) kuerzel,
                        c.company_no                                             endkunde_nr,
                        c.y_evt_pa_aussetzen                                     vb_filter_von,
                        c.y_evt_bis                                              vb_filter_bis,
                        c.y_grund                                                vb_filter_grund
                FROM bsi_communication cm
                JOIN bsi_company       c   ON cm.company_nr  = c.company_nr
                WHERE cm.evt_start >= to_date('01.01.2009', 'dd.mm.yyyy')
                ORDER BY c.company_no, cm.evt_start
            ''',
    
            ####################################################################################################
            bd = '''
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
                    JOIN flaechen_denorm_tot_v fl ON fz.fl_oid   = fl.fl_oid         AND TRUNC(fz.aushang_beginn) BETWEEN fl.gueltig_von AND fl.gueltig_bis
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
                            CASE WHEN TO_CHAR(ap.reservation_datum, 'iw') = 53 THEN '52' ELSE TO_CHAR(ap.reservation_datum, 'iw') END RES_KW,                              
                            TRUNC(ap.aushang_beginn)                            AUSH_vON,
                            TO_CHAR(ap.aushang_beginn, 'iyyy')                  AUSH_JAHR,
                            CASE WHEN TO_CHAR(ap.aushang_beginn, 'iw') = 53 THEN '52' ELSE TO_CHAR(ap.aushang_beginn, 'iw') END AUSH_KW,  
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
                                                                                                                                    
                    JOIN subjekte_denorm_tot_v    au  ON kv.atgeb_id              = au.atgeb_id  AND TRUNC(kv.atgeb_date)         BETWEEN au.gueltig_von  AND au.gueltig_bis    
                    JOIN subjekte_denorm_tot_v    op  ON kv.vkoe_oid              = op.vkoe_oid  AND TRUNC(kv.vkoe_date)          BETWEEN op.gueltig_von  AND op.gueltig_bis
                    JOIN subjekte_denorm_tot_v    VB  ON kv.vkber_oid             = vb.vkber_oid AND TRUNC(kv.vkber_date)         BETWEEN vb.gueltig_von  AND vb.gueltig_bis
                    JOIN subjekte_denorm_tot_v    hb  ON kv.vkber_hbe_oid         = hb.vkber_oid AND TRUNC(kv.vkber_hbe_date)     BETWEEN hb.gueltig_von  AND hb.gueltig_bis
                LEFT JOIN subjekte_denorm_tot_v    wa  ON kv.werbeagentur_subj_oid = wa.subj_oid  AND TRUNC(kv.werbe_agentur_date) BETWEEN wa.gueltig_von  AND wa.gueltig_bis 
                                                            
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
            '''
        )