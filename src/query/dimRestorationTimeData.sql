-- dim restoration
WITH SLA AS (
    SELECT
        PI.PROD_INSTNC_KEY AS PROD_INSTNC_KEY,
        case
            when PI.ENHNC_CUST_SRVC_ID is null then null
            else PI.PROD_INSTNC_KEY
        end AS ENHNC_CUST_SRVC_ID,
        --PI.ENHNC_CUST_SRVC_ID,
        -- PI.PROD_EXTRNL_ALIAS_EN_NM     AS PROD_EXTRNL_ALIAS_NM,
        'MSU_NM_' || TO_CHAR(PI.PROD_INSTNC_KEY) AS PROD_EXTRNL_ALIAS_NM,
        PI.SRVC_TYP_EN_NM AS SRVC_TYP_NM,
        --PROD_INSTNC_EN_NM              AS PROD_INSTNC_NM,
        'PROD_INSTNC_EN_NM_' || TO_CHAR(PI.PROD_INSTNC_KEY) AS PROD_INSTNC_NM,
        PI.SRVC_SUB_TYP_EN_NM AS SRVC_SUB_TYP_NM,
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
        SRAM.SLA_MNTH,
        PI.REPORTABLE_SRVC_IND AS REPORTABLE_SRVC_IND,
        PI.BILLING_START_TS,
        PI.BILLING_STOP_TS
    FROM
        SLA_MTRS_MNTHLY SRAM
        JOIN PROD_INSTNC pi ON sram.PROD_INSTNC_KEY = pi.PROD_INSTNC_KEY
    WHERE
        SRAM.SLA_MNTH BETWEEN TO_DATE (
            '05/01/2022 00:00:00',
            'MM/DD/YYYY HH24:MI:SS'
        )
        AND TO_DATE (
            '10/31/2023 23:59:59',
            'MM/DD/YYYY HH24:MI:SS'
        )
        AND pi.STAT_TYP_EN_NM != 'Delete'
        AND (
            SRAM.SLA_MNTH BETWEEN GREATEST (
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
),
prod_locn_with AS (
    SELECT
        *
    FROM
        (
            SELECT
                pla.PROD_INSTNC_KEY,
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
                ROW_NUMBER () OVER (
                    PARTITION BY pla.PROD_INSTNC_KEY
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
                pcca.PROD_INSTNC_KEY,
                pcca.CUST_ORG_KEY,
                pcca.EFFECTIVE_START_TS,
                pcca.EFFECTIVE_END_TS,
                SLA_MNTH,
                CORG.CUST_ORG_LEVEL_TYP,
                CUST_ORG_EN_NM,
                ROW_NUMBER () OVER (
                    PARTITION BY pcca.PROD_INSTNC_KEY
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
                PCCW.PROD_INSTNC_KEY,
                PCCW.CUST_ORG_LEVEL_TYP,
                CUST_ORG_EN_NM,
                COA.CUST_ORG_ASSN_KEY,
                COA.TO_CUST_ORG_KEY,
                COA.FROM_CUST_ORG_KEY,
                COA.EFFECTIVE_START_TS,
                COA.EFFECTIVE_END_TS,
                ROW_NUMBER () OVER (
                    PARTITION BY PCCW.PROD_INSTNC_KEY,
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
        PROD_INSTNC_KEY,
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
    SLA_MNTH,
    SLA.PROD_INSTNC_KEY,
    ENHNC_CUST_SRVC_ID,
    PROD_EXTRNL_ALIAS_NM,
    SRVC_TYP_NM,
    PROD_INSTNC_NM,
    SRVC_SUB_TYP_NM,
    BILLING_START_TS,
    BILLING_STOP_TS,
    --SLA.MTRS_THRSHLD_HRS_QTY,
    --SLT_MSR_MTHD_NM,
    --MTRS_PASS_IND,
    --FAILED_INCIDENT_QTY,
    PROV_STAT_CD,
    MUNIC_NM,
    LONG_ADDR_STR,
    POSTAL_ZIP_CD,
    ZONE_NM,
    AGENCY,
    MINISTRY,
    CLUST
FROM
    sla
    LEFT JOIN prod_locn_with plw ON sla.PROD_INSTNC_KEY = plw.PROD_INSTNC_KEY
    JOIN hierarchical_org_with how ON sla.PROD_INSTNC_KEY = how.PROD_INSTNC_KEY
WHERE
    1 = 1
    AND CLUST IS NOT NULL
    AND MINISTRY IS NOT NULL
    AND REPORTABLE_SRVC_IND = 'Y'
ORDER BY
    PROD_INSTNC_KEY,
    SLA_MNTH
    /*ORDER BY CLUST,
     
     MINISTRY,
     
     PROV_STAT_CD,
     
     MUNIC_NM,
     
     LONG_ADDR_STR,
     
     POSTAL_ZIP_CD*/