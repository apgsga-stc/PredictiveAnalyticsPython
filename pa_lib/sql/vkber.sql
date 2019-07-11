SELECT kombi_name, e_mail_1 e_mail, kurzzeichen, ma_funktion funktion
  FROM subjekte_denorm_tot_v ekx 
 WHERE ist_vkber = 1
   AND austrittsdatum is null
   AND ma_funktion is not null
   AND (SELECT max(su.gueltig_von) 
          FROM subjekte_denorm_tot_v su 
         WHERE su.subj_oid = ekx.subj_oid) BETWEEN ekx.gueltig_von  AND ekx.gueltig_bis