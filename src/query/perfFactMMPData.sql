WITH SLA AS (
    SELECT
        DISTINCT perf_fact.CE_PROD_INSTNC_KEY as Prod_instnc_key,
        perf_fact.SAMP_CUST_RQST_TS AS SLA_MNTH,
        SRVC_CLASSN_CD,
        STST.SLA_TST_STAT_TYP_CD AS SLA_STAT_TYP_CD,
        TOT_PACKET_LOST_QTY AS NUM_OF_PACKET_LOSS,
        SLA_PERF_TEST_PATH_CNTRL_KEY,
        PERF_FACT.SLA_TYP
    FROM
        SLA_PERF_MNTHLY perf_fact
        LEFT JOIN SLA_TST_STAT_TYP STST ON PERF_FACT.SLA_TST_STAT_TYP_KEY = STST.SLA_TST_STAT_TYP_KEY
    WHERE
        perf_fact.SAMP_CUST_RQST_TS BETWEEN TO_DATE (:startTs, 'MM/DD/YYYY HH24:MI:SS') -- Reporting start date - parameter
        AND TO_DATE (:endTs, 'MM/DD/YYYY HH24:MI:SS') -- Reporting end date - parameter
        and SLA_PERF_TEST_PATH_CNTRL_KEY is not null --AND PERF_FACT.SLA_TYP = 'SLT-PTD'  -- Parameter -  KPI : SLT-PTD, SLT-DDR or SLT-JITTER(?)
),
perf_sla_hist AS (
    SELECT
        SAMP_CUST_RQST_TS,
        CE_PROD_INSTNC_KEY,
        sla_typ,
        SLA_PERF_TEST_PATH_CNTRL_KEY,
        SRVC_CLASSN_CD,
        sla_tst_stat_typ_key,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            1
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag1,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            2
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag2,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            3
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag3,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            4
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag4,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            5
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag5,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            6
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag6,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            7
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag7,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            8
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag8,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            9
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag9,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            10
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag10,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            11
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag11,
        LAG (
            DECODE (
                sla_tst_stat_typ_key,
                0,
                'PASS',
                1,
                'FAIL',
                2,
                'PASS',
                3,
                'PASS',
                5,
                'FAIL',
                'NA'
            ),
            12
        ) OVER (
            PARTITION BY CE_PROD_INSTNC_KEY,
            sla_typ,
            SLA_PERF_TEST_PATH_CNTRL_KEY,
            SRVC_CLASSN_CD
            ORDER BY
                CE_PROD_INSTNC_KEY,
                sla_typ,
                SLA_PERF_TEST_PATH_CNTRL_KEY,
                SRVC_CLASSN_CD,
                SAMP_CUST_RQST_TS
        ) AS sla_tst_stat_typ_key_lag12
    FROM
        SLA_PERF_MNTHLY
    WHERE
        SAMP_CUST_RQST_TS <= TO_DATE ('2024/03/01', 'YYYY/MM/DD')
        AND SAMP_CUST_RQST_TS >= TO_DATE ('2024/01/01', 'YYYY/MM/DD') - INTERVAL '15' MONTH
        and SLA_PERF_TEST_PATH_CNTRL_KEY is not null
    ORDER BY
        2,
        3,
        1
),
Final_hist AS (
    SELECT
        SAMP_CUST_RQST_TS,
        CE_PROD_INSTNC_KEY,
        sla_typ,
        SLA_PERF_TEST_PATH_CNTRL_KEY,
        SRVC_CLASSN_CD,
        sla_tst_stat_typ_key,
        NVL (sla_tst_stat_typ_key_lag12, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag11, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag10, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag9, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag8, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag7, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag6, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag5, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag4, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag3, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag2, 'NA') || '/' || NVL (sla_tst_stat_typ_key_lag1, 'NA') AS history
    FROM
        perf_sla_hist
)
SELECT
    sl.PROD_INSTNC_KEY,
    sl.SLA_MNTH,
    TO_CHAR (sl.SLA_MNTH, 'MM/DD/YYYY') AS SAMP_REQUEST_TS,
    sl.SRVC_CLASSN_CD,
    sl.SLA_STAT_TYP_CD,
    sl.SLA_TYP,
    (
        CASE
            WHEN sl.SLA_STAT_TYP_CD IS NULL
            OR UPPER (sl.SLA_STAT_TYP_CD) = 'N/A' THEN 'N/A'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'PASSED' THEN 'Y'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'FAILED' THEN 'N'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'FAILED-OMISSION' THEN 'O'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'EXCLUDED-UTILIZ' THEN 'U'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'EXCLUDED-NOT-REACH' THEN 'U'
            WHEN UPPER (sl.SLA_STAT_TYP_CD) = 'EXCLUDED-INCIDENT' THEN 'A'
            ELSE 'N/A'
        END
    ) AS SLT_INDICATOR,
    sl.SLA_PERF_TEST_PATH_CNTRL_KEY,
    s.history
FROM
    SLA sl
    join Final_hist s on sl.sla_mnth = s.samp_cust_rqst_ts
    and sl.Prod_instnc_key = s.CE_PROD_INSTNC_KEY
    and sl.sla_typ = s.sla_typ
    and sl.SLA_PERF_TEST_PATH_CNTRL_KEY = s.SLA_PERF_TEST_PATH_CNTRL_KEY
    and sl.SRVC_CLASSN_CD = s.SRVC_CLASSN_CD
order by
    1,
    7,
    4,
    6,
    2