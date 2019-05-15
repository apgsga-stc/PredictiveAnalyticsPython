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
        to_char(substr(c.y_grund, 1, 4000))                      vb_filter_grund
FROM bsi_communication cm
JOIN bsi_company       c   ON cm.company_nr  = c.company_nr
WHERE cm.evt_start >= to_date('01.01.2009', 'dd.mm.yyyy')
ORDER BY c.company_no, cm.evt_start
