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
        nvl(cc.company_no, c.company_no)                         endkunde_nr,
        (case when cc.company_no != c.company_no
              then 'agentur' else 'direkt' end)                  typ,
        (case when cc.company_no != c.company_no
              then c.company_no else null end)                   agentur_nr,
        (case when cc.company_no != c.company_no
              then cc.y_evt_pa_aussetzen
              else c.y_evt_pa_aussetzen end)                     vb_filter_von,
        (case when cc.company_no != c.company_no
              then cc.y_evt_bis else c.y_evt_bis end)            vb_filter_bis,
        (case when cc.company_no != c.company_no
              then to_char(substr(cc.y_grund, 1, 4000))
              else to_char(substr(c.y_grund, 1, 4000)) end)      vb_filter_grund
FROM bsi_communication      cm
JOIN bsi_company            c  ON cm.company_nr            = c.company_nr
LEFT OUTER JOIN bsi_company cc ON cm.x_concern_company_nr  = cc.company_nr
WHERE cm.evt_start >= to_date('01.01.2009', 'dd.mm.yyyy')
