-- MTRS fact query: 
WITH SLA AS (
    SELECT
        SRAM.PROD_INSTNC_KEY,
        SRAM.MTRS_THRSHLD_HRS_QTY,
        (
            CASE
                WHEN SRVC_TYP_EN_NM = 'CORE/MEET-ME-POINT SERVICE' THEN 'N/A'
                WHEN SRAM.FAILED_INCIDENT_QTY > 0 THEN 'N'
                ELSE 'Y'
            END
        ) AS MTRS_PASS_IND,
        SRAM.SLT_MSR_MTHD_NM,
        SRAM.FAILED_INCIDENT_QTY,
        SRAM.SLA_MNTH
    FROM
        SLA_MTRS_MNTHLY SRAM
        JOIN PROD_INSTNC PI ON SRAM.prod_instnc_key = PI.prod_instnc_key
    WHERE
        SRAM.SLA_MNTH BETWEEN TO_DATE (
            '10/01/2022 00:00:00',
            'MM/DD/YYYY HH24:MI:SS'
        )
        AND TO_DATE (
            '12/31/2023 23:59:59',
            'MM/DD/YYYY HH24:MI:SS'
        )
),
avail_sla_hist AS (
    SELECT
        SRAM.PROD_INSTNC_KEY,
        --                SRAM.TOT_OUTAGE_HRS_QTY,
        --                SRAM.AVAIL_PCT,
        --                SRAM.AVAIL_THRSHLD_PCT,
        --                SRAM.SLT_MSR_MTHD_NM,
        SRAM.MTRS_PASS_IND,
        --                SRAM.TOTAL_INCIDENT_COUNT,
        SRAM.SLA_MNTH,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            1
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag1,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            2
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag2,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            3
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag3,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            4
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag4,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            5
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag5,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            6
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag6,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            7
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag7,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            8
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag8,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            9
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag9,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            10
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag10,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            11
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag11,
        LAG (
            DECODE (
                MTRS_PASS_IND,
                'Y',
                'PASS',
                'N',
                'FAIL',
                'NA'
            ),
            12
        ) OVER (
            PARTITION BY PROD_INSTNC_KEY
            ORDER BY
                PROD_INSTNC_KEY,
                SLA_MNTH
        ) AS sla_tst_stat_typ_key_lag12
    FROM
        SLA SRAM
    WHERE
        SRAM.SLA_MNTH <= TO_DATE ('2023/12/01', 'YYYY/MM/DD')
        AND SRAM.SLA_MNTH >= TO_DATE ('2023/12/01', 'YYYY/MM/DD') - INTERVAL '15' MONTH --                    AND PROD_INSTNC_KEY = 378002
    ORDER BY
        4
),
Final_hist AS (
    SELECT
        SLA_MNTH,
        PROD_INSTNC_KEY,
        MTRS_PASS_IND,
        NVL (sla_tst_stat_typ_key_lag12, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag11, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag10, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag9, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag8, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag7, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag6, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag5, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag4, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag3, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag2, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag1, 'NA') AS history
    FROM
        avail_sla_hist
),
sla_hist AS (
    SELECT
        *
    FROM
        final_hist
    WHERE
        SLA_MNTH <= TO_DATE ('2023/12/01', 'YYYY/MM/DD')
        AND SLA_MNTH >= TO_DATE ('2023/12/01', 'YYYY/MM/DD') - INTERVAL '3' MONTH
) --           SELECT DISTINCT * FROM sla_hist WHERE PROD_INSTNC_KEY = 378002
SELECT
    DISTINCT SLA.*,
    sla_hist.HISTORY
FROM
    SLA,
    sla_hist
WHERE
    sla_hist.PROD_INSTNC_KEY = sla.PROD_INSTNC_KEY
    AND sla_hist.SLA_MNTH = sla.SLA_MNTH
order by
    sla.prod_instnc_key,
    sla.sla_mnth