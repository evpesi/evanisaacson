WITH 
--First CTE
rate_of_readmission_by_state AS (
SELECT state, ROUND(AVG(TRY_TO_DECIMAL(score, 3, 1)), 3) as Avg_Score
  FROM CMS_DB.PROD.CMS_UNPLANNED_HOSPITAL_VISITS
 WHERE MEASURE_NAME = 'Rate of readmission after discharge from hospital (hospital-wide)' 
			 AND score != 'Not Available'
 GROUP BY state
 ORDER BY Avg_Score ASC
),
--Second CTE
rate_of_readmission AS (
SELECT *, TRY_TO_DECIMAL(score, 3, 1) as Score_Decimal
  FROM CMS_DB.PROD.CMS_UNPLANNED_HOSPITAL_VISITS
 WHERE MEASURE_NAME = 'Rate of readmission after discharge from hospital (hospital-wide)' 
			 AND score != 'Not Available'
)

--Query 
SELECT *, 
       ROUND(Avg_Score - (SELECT AVG(score) 
                            FROM rate_of_readmission),3) AS Diff_Natl
  FROM rate_of_readmission_by_state
 LIMIT 5;