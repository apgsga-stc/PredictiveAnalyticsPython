SELECT
    ma_id,
    kombi_name,
    e_mail_1      e_mail,
    kurzzeichen,
    (SELECT 1
       FROM dual
      WHERE EXISTS (
                SELECT NULL
                FROM vb_vkoe_suche_v
                WHERE ma_id = ekx.ma_id
                  AND opoe_bezeichnung LIKE 'KAM Verkauf %'
            )
    ) ist_kam,
    ma_funktion   funktion
FROM
    subjekte_denorm_tot_v ekx
WHERE
    ist_vkber = 1
    AND austrittsdatum IS NULL
    AND ma_funktion IS NOT NULL
    AND (
        SELECT MAX(su.gueltig_von)
        FROM subjekte_denorm_tot_v su
        WHERE su.subj_oid = ekx.subj_oid
    ) BETWEEN ekx.gueltig_von AND ekx.gueltig_bis