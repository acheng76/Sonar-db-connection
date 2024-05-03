WITH SLA AS (
    SELECT
        DISTINCT pi.PROD_INSTNC_KEY AS PROD_INSTNC_KEY,
        PI.SRVC_TYP_EN_NM AS SRVC_TYP_NM,
        PI.PROD_CATLG_EN_NM AS PROD_CATLG_NM,
        'PROD_INSTNC_EN_NM_' || pi.prod_instnc_key AS PROD_INSTNC_NM,
        PI.SRVC_SUB_TYP_EN_NM AS SRVC_SUB_TYP_NM,
        perf_fact.SAMP_CUST_RQST_TS AS SLA_MNTH,
        --SRVC_CLASSN_CD,
        --STST.SLA_TST_STAT_TYP_CD AS SLA_STAT_TYP_CD,
        --TOT_PACKET_LOST_QTY AS NUM_OF_PACKET_LOSS,
        SLA_PERF_TEST_PATH_CNTRL_KEY,
        'PROD_EXTRNL_ALIAS_EN_NM_' || pi.prod_instnc_key AS PROD_EXTRNL_ALIAS_NM
    FROM
        SLA_PERF_MNTHLY perf_fact
        JOIN PROD_INSTNC PI ON PI.PROD_INSTNC_KEY = perf_fact.CE_PROD_INSTNC_KEY
        LEFT JOIN SLA_TST_STAT_TYP STST ON PERF_FACT.SLA_TST_STAT_TYP_KEY = STST.SLA_TST_STAT_TYP_KEY
    WHERE
        perf_fact.SAMP_CUST_RQST_TS BETWEEN TO_DATE (:startTs, 'MM/DD/YYYY HH24:MI:SS') -- Reporting start date - parameter
        AND TO_DATE (:endTs, 'MM/DD/YYYY HH24:MI:SS') -- Reporting end date - parameter
        AND PERF_FACT.SLA_TYP = :slaType -- Parameter -  KPI : SLT-PTD, SLT-DDR or SLT-JITTER(?)
        AND pi.STAT_TYP_EN_NM != 'Delete'
        and SLA_PERF_TEST_PATH_CNTRL_KEY is not null
        AND (
            perf_fact.SAMP_CUST_RQST_TS BETWEEN GREATEST (
                PI.BILLING_START_TS,
                NVL (
                    PI.LAN_REPORT_START_TS,
                    TO_DATE (
                        '1666-06-06',
                        'YYYY-MM-DD'
                    )
                )
            )
            AND FROM_TZ (
                CAST (
                    pi.BILLING_STOP_TS AS TIMESTAMP
                ),
                'UTC'
            ) AT TIME ZONE 'America/New_York'
        )
        AND PI.REPORTABLE_SRVC_IND = 'Y'
),
prod_locn_with AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT pla.PROD_INSTNC_KEY,
                sla.SLA_MNTH,
                pla.LOCN_KEY,
                pla.EFFECTIVE_START_TS,
                pla.EFFECTIVE_END_TS,
                PROV_STAT_CD,
                --MUNIC_NM,
                'MUNIC_NM_' || TO_CHAR(pla.LOCN_KEY) AS MUNIC_NM,
                --LONG_ADDR_STR,
                'LONG_ADDR_STR_' || TO_CHAR(pla.LOCN_KEY) AS LONG_ADDR_STR,
                --POSTAL_ZIP_CD,
                'POSTAL_ZIP_CD_' || TO_CHAR(pla.LOCN_KEY) AS POSTAL_ZIP_CD,
                --ZONE_NM,
                'ZONE_NM_' || TO_CHAR(pla.LOCN_KEY) AS ZONE_NM,
                RANK () OVER (
                    PARTITION BY pla.PROD_INSTNC_KEY,
                    sla.SLA_MNTH
                    ORDER BY
                        pla.EFFECTIVE_START_TS,
                        pla.PROD_LOCN_ASSN_KEY ASC
                ) RN
            FROM
                PROD_LOCN_ASSN pla
                JOIN sla ON sla.PROD_INSTNC_KEY = pla.PROD_INSTNC_KEY
                JOIN LOCN loc ON pla.LOCN_KEY = loc.LOCN_KEY
                AND (
                    sla.SLA_MNTH BETWEEN TRUNC (
                        CAST (
                            FROM_TZ (
                                CAST (
                                    pla.EFFECTIVE_START_TS AS TIMESTAMP
                                ),
                                'UTC'
                            ) AT TIME ZONE 'America/New_York' AS DATE
                        ),
                        'MM'
                    )
                    AND FROM_TZ (
                        CAST (
                            pla.EFFECTIVE_END_TS AS TIMESTAMP
                        ),
                        'UTC'
                    ) AT TIME ZONE 'America/New_York'
                )
                AND (
                    sla.SLA_MNTH BETWEEN TRUNC (
                        CAST (
                            FROM_TZ (
                                CAST (
                                    loc.EFFECTIVE_START_TS AS TIMESTAMP
                                ),
                                'UTC'
                            ) AT TIME ZONE 'America/New_York' AS DATE
                        ),
                        'MM'
                    )
                    AND FROM_TZ (
                        CAST (
                            loc.EFFECTIVE_END_TS AS TIMESTAMP
                        ),
                        'UTC'
                    ) AT TIME ZONE 'America/New_York'
                )
        )
    WHERE
        RN = 1
),
prod_cust_cost_with AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT pcca.PROD_INSTNC_KEY,
                SLA_MNTH,
                pcca.CUST_ORG_KEY,
                pcca.EFFECTIVE_START_TS,
                pcca.EFFECTIVE_END_TS,
                CORG.CUST_ORG_LEVEL_TYP,
                CUST_ORG_EN_NM,
                COST_ALLOC_PCT,
                RANK () OVER (
                    PARTITION BY pcca.PROD_INSTNC_KEY,
                    SLA_MNTH
                    ORDER BY
                        pcca.EFFECTIVE_START_TS,
                        pcca.PROD_CUST_COST_ALLOC_KEY ASC
                ) RN
            FROM
                PROD_CUST_COST_ALLOC pcca
                JOIN sla ON sla.PROD_INSTNC_KEY = pcca.PROD_INSTNC_KEY
                JOIN cust_org corg ON PCCA.CUST_ORG_KEY = CORG.CUST_ORG_KEY
                AND (
                    sla.SLA_MNTH BETWEEN TRUNC (
                        CAST (
                            FROM_TZ (
                                CAST (
                                    pcca.EFFECTIVE_START_TS AS TIMESTAMP
                                ),
                                'UTC'
                            ) AT TIME ZONE 'America/New_York' AS DATE
                        ),
                        'MM'
                    )
                    AND FROM_TZ (
                        CAST (
                            pcca.EFFECTIVE_END_TS AS TIMESTAMP
                        ),
                        'UTC'
                    ) AT TIME ZONE 'America/New_York'
                )
        )
    WHERE
        RN = 1
),
cust_org_assn_with AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT PCCW.PROD_INSTNC_KEY,
                PCCW.SLA_MNTH,
                PCCW.CUST_ORG_LEVEL_TYP,
                CUST_ORG_EN_NM,
                COA.CUST_ORG_ASSN_KEY,
                COA.TO_CUST_ORG_KEY,
                COA.FROM_CUST_ORG_KEY,
                COA.EFFECTIVE_START_TS,
                COA.EFFECTIVE_END_TS,
                COST_ALLOC_PCT,
                RANK () OVER (
                    PARTITION BY PCCW.PROD_INSTNC_KEY,
                    PCCW.SLA_MNTH,
                    COA.TO_CUST_ORG_KEY
                    ORDER BY
                        COA.EFFECTIVE_START_TS,
                        COA.FROM_CUST_ORG_KEY ASC
                ) RN
            FROM
                CUST_ORG_ASSN COA
                JOIN prod_cust_cost_with pccw ON pccw.CUST_ORG_KEY = COA.TO_CUST_ORG_KEY
            WHERE
                (
                    pccw.SLA_MNTH BETWEEN TRUNC (
                        CAST (
                            FROM_TZ (
                                CAST (
                                    COA.EFFECTIVE_START_TS AS TIMESTAMP
                                ),
                                'UTC'
                            ) AT TIME ZONE 'America/New_York' AS DATE
                        ),
                        'MM'
                    )
                    AND FROM_TZ (
                        CAST (
                            COA.EFFECTIVE_END_TS AS TIMESTAMP
                        ),
                        'UTC'
                    ) AT TIME ZONE 'America/New_York'
                )
        )
    WHERE
        RN = 1
),
hierarchical_org_with AS (
    SELECT
        DISTINCT PROD_INSTNC_KEY,
        COST_ALLOC_PCT,
        SLA_MNTH,
        (
            CASE
                WHEN UPPER (CUST_ORG_LEVEL_TYP) = 'AGENCY' THEN 'AGENCY_NM_' || TO_CHAR(TO_CUST_ORG_KEY)
            END
        ) AS AGENCY,
        (
            CASE
                WHEN UPPER (CUST_ORG_LEVEL_TYP) = 'MINISTRY' THEN 'MINISTRY_NM_' || TO_CHAR(TO_CUST_ORG_KEY)
                WHEN UPPER (CUST_ORG_LEVEL_TYP) = 'AGENCY' THEN (
                    SELECT
                        'MINISTRY_NM_' || TO_CHAR(FROM_CUST_ORG_KEY)
                    FROM
                        CUST_ORG_ASSN
                        JOIN CUST_ORG ON CUST_ORG_KEY = FROM_CUST_ORG_KEY
                    WHERE
                        UPPER (CUST_ORG_LEVEL_TYP) = 'MINISTRY'
                        AND ROWNUM = 1 CONNECT BY NOCYCLE PRIOR FROM_CUST_ORG_KEY = TO_CUST_ORG_KEY START WITH CUST_ORG_ASSN_KEY = cust_org_assn_with.CUST_ORG_ASSN_KEY
                )
            END
        ) AS MINISTRY,
        (
            SELECT
                'CLUSTER_NM_' || TO_CHAR(FROM_CUST_ORG_KEY)
            FROM
                CUST_ORG_ASSN
                JOIN CUST_ORG ON CUST_ORG_KEY = FROM_CUST_ORG_KEY
            WHERE
                UPPER (CUST_ORG_LEVEL_TYP) = 'CLUSTER'
                AND ROWNUM = 1 CONNECT BY NOCYCLE PRIOR FROM_CUST_ORG_KEY = TO_CUST_ORG_KEY START WITH CUST_ORG_ASSN_KEY = cust_org_assn_with.CUST_ORG_ASSN_KEY
        ) AS CLUST
    FROM
        cust_org_assn_with
)
SELECT
    DISTINCT SLA.SLA_MNTH,
    SLA.PROD_INSTNC_KEY,
    SLA.PROD_CATLG_NM,
    TO_CHAR (SLA.SLA_MNTH, 'MM/DD/YYYY') AS SAMP_REQUEST_TS,
    PROD_EXTRNL_ALIAS_NM,
    SRVC_TYP_NM,
    PROD_INSTNC_NM,
    SRVC_SUB_TYP_NM,
    PROV_STAT_CD,
    MUNIC_NM,
    LONG_ADDR_STR,
    POSTAL_ZIP_CD,
    ZONE_NM,
    AGENCY,
    MINISTRY,
    CLUST,
    COST_ALLOC_PCT,
    'SRC_MSU_ID_' || RESRC.RESRC_DVC_INSTNC_KEY AS SRC_MSU_ID,
    'DEST_MSU_ID_' || TARGET.RESRC_DVC_INSTNC_KEY AS DEST_MSU_ID,
    TEST_PATH_TYP_CD AS MMP_TYPE,
    sla.SLA_PERF_TEST_PATH_CNTRL_KEY
FROM
    sla
    LEFT JOIN prod_locn_with plw ON sla.PROD_INSTNC_KEY = plw.PROD_INSTNC_KEY
    AND sla.SLA_MNTH = plw.SLA_MNTH
    LEFT JOIN hierarchical_org_with how ON sla.PROD_INSTNC_KEY = how.PROD_INSTNC_KEY
    AND sla.SLA_MNTH = how.SLA_MNTH
    LEFT JOIN SLA_PERF_TEST_PATH_CNTRL tpc ON tpc.SLA_PERF_TEST_PATH_CNTRL_KEY = sla.SLA_PERF_TEST_PATH_CNTRL_KEY
    LEFT JOIN RESRC_DVC_INSTNC RESRC ON RESRC.RESRC_DVC_INSTNC_KEY = SRC_RESRC_DVC_INSTNC_KEY
    LEFT JOIN RESRC_DVC_INSTNC TARGET ON TARGET.RESRC_DVC_INSTNC_KEY = DST_RESRC_DVC_INSTNC_KEY
WHERE
    1 = 1
    AND CLUST IS NOT NULL
    AND MINISTRY IS NOT NULL
    AND MUNIC_NM IS NOT NULL
order by
    1,
    2