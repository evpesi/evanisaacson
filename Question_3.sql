 SELECT c.state, 
       ROUND(((COUNT(*) / (t.state_row_count * 1.0))*100.0), 2) AS Not_Avail_Proportion_Pct
  FROM CMS_DB.PROD.CMS_UNPLANNED_HOSPITAL_VISITS c 
  JOIN (SELECT state, COUNT(*) AS state_row_count 
          FROM CMS_DB.PROD.CMS_UNPLANNED_HOSPITAL_VISITS 
         GROUP BY state) AS t
    ON c.state = t.state
 WHERE score = 'Not Available'
 GROUP BY c.state, t.state_row_count
 ORDER BY Not_Avail_Proportion_Pct ASC;