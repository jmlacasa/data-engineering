CREATE OR REPLACE PROCEDURE public.petl_desastres()
 LANGUAGE sql
AS $$
DELETE FROM desastres.public.desastres_final;
INSERT INTO desastres.public.desastres_final (
SELECT 
(c.año+1)/4 * 4 - 1 as cuatrenio
, AVG(c.temperatura) Temp_AVG
, AVG(c.oxigeno) Oxi_AVG
, SUM(d.tsunamis) T_Tsunamis 
, SUM(d.olas_calor) T_OlasCalor
, SUM(d.terremotos) T_Terremotos
, SUM(d.erupciones) T_Erupciones
, SUM(d.incendios) T_Incendios
, AVG(m.r_menor15 + m.r_15_a_30) M_Jovenes_AVG
, AVG(m.r_30_a_45 + m.r_45_a_60) M_Adutos_AVG 
, AVG(m.r_m_a_60) M_Ancianos_AVG
FROM desastres.public.clima c 
INNER JOIN desastres d 
	ON d.año = c.año 
INNER JOIN muertes m 
	ON m.año = c.año
GROUP BY cuatrenio
);
$$
;
