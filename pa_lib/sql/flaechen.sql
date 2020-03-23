select *
  from fm.flaechen_denorm_t
 where ist_operative_flaeche = 1
   and sprache_id = 1
   and gueltig_bis >= to_date('01.01.2016', 'dd.mm.yyyy')
   and gueltig_von <= to_date('31.12.2019', 'dd.mm.yyyy')
