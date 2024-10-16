SELECT CG.CUST_GRP_ID,
       CG.CUST_GRP_NM,
       C.CUST_ID,
       C.CUST_CD,
       NVL (CUST_TRNSLT_NM, C.CUST_DESC_TXT) AS CUST_TRNSLT_NM,
       C.CUST_DESC_TXT
  FROM CUST_GRP CG
       LEFT JOIN CUST_GRP_CUST_ASSN CGCA
          ON     CG.CUST_GRP_ID = CGCA.CUST_GRP_ID
             AND SYS_EXTRACT_UTC (SYSTIMESTAMP) BETWEEN CGCA.EFF_START_TS
                                                    AND CGCA.EFF_END_TS
       LEFT JOIN CUST C
          ON     CGCA.CUST_ID = C.CUST_ID
             AND SYS_EXTRACT_UTC (SYSTIMESTAMP) BETWEEN C.EFF_START_TS
                                                    AND C.EFF_END_TS
       LEFT JOIN CUST_NM_TRNSLTN CT
          ON     C.CUST_ID = CT.CUST_ID
             AND SYS_EXTRACT_UTC (SYSTIMESTAMP) BETWEEN CT.EFF_START_TS
                                                    AND CT.EFF_END_TS
             AND CT.LANG_ID is not null   -- Parameter 
       JOIN CUST_GRP_USER_ASSN CGUA ON CGUA.CUST_GRP_ID = CG.CUST_GRP_ID
 WHERE     CGUA.APPL_USER_ID = :appuserID-- 5440-- Parameter
       AND SYS_EXTRACT_UTC (SYSTIMESTAMP) BETWEEN CG.EFF_START_TS
                                              AND CG.EFF_END_TS
       AND CGUA.ADMIN_IND is not null -- Parameter