-- =============================================================================
-- SHBP GLP-1 Continuation Dashboard
-- Description : Member-level continuation snapshot powering the SHBP GLP-1
--               Continuation Tableau dashboard. One row per active member.
-- Database    : nineamdwh
-- Refreshed   : Daily (Tableau extract)
-- Author      : Megan Riddle
-- =============================================================================


-- =============================================================================
-- QUERY 1: continuation members (primary source)
-- Grain       : One row per active member
-- Joins to    : continuation_answers (secondary) on readable_id
-- =============================================================================

WITH

-- ---------------------------------------------------------------------------
-- Base population: all users with an active, non-cancelled subscription
-- ---------------------------------------------------------------------------
active_members AS (
    SELECT
        u.id                                                AS member_id,
        u.readable_id,
        u.primary_condition_group,
        s.subscription_start_date,
        DATEDIFF(CURDATE(), s.subscription_start_date)      AS days_enrolled
    FROM users u
    JOIN (
        SELECT
            user_id,
            MIN(start_date)        AS subscription_start_date,
            MAX(cancellation_date) AS cancellation_date
        FROM subscriptions
        WHERE status = 'ACTIVE'
          AND cancellation_date IS NULL
        GROUP BY user_id
    ) s ON s.user_id = u.id
),

-- ---------------------------------------------------------------------------
-- Employer: most recent active subscription employer per member
-- ---------------------------------------------------------------------------
employer AS (
    SELECT member_id, employer_name
    FROM (
        SELECT
            pe.user_id                                      AS member_id,
            pe.name                                         AS employer_name,
            ROW_NUMBER() OVER (
                PARTITION BY pe.user_id
                ORDER BY s.start_date DESC
            )                                               AS rn
        FROM partner_employers pe
        JOIN subscriptions s
            ON  s.user_id           = pe.user_id
            AND s.status            = 'ACTIVE'
            AND s.cancellation_date IS NULL
    ) r
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- Payer: most recent active subscription payer per member
-- ---------------------------------------------------------------------------
payer AS (
    SELECT member_id, payer_name
    FROM (
        SELECT
            pp.user_id                                      AS member_id,
            pp.name                                         AS payer_name,
            ROW_NUMBER() OVER (
                PARTITION BY pp.user_id
                ORDER BY s.start_date DESC
            )                                               AS rn
        FROM partner_payers pp
        JOIN subscriptions s
            ON  s.user_id           = pp.user_id
            AND s.status            = 'ACTIVE'
            AND s.cancellation_date IS NULL
    ) r
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- GLP-1 past use (question A8z9j98E0sxR)
-- answer_value = 1 → continuation member
-- ---------------------------------------------------------------------------
glp1_past_use AS (
    SELECT member_id, past_glp1_use
    FROM (
        SELECT
            user_id      AS member_id,
            answer_value AS past_glp1_use,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY answered_at DESC
            )            AS rn
        FROM questionnaire_records
        WHERE question_id    = 'A8z9j98E0sxR'
          AND is_latest_answer = 1
    ) r
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- Continuation intent (question gV9Xu8RzF9hR)
-- Captures whether member wants to continue GLP-1 medication
-- ---------------------------------------------------------------------------
continuation_answer AS (
    SELECT member_id, wants_to_continue_glp1
    FROM (
        SELECT
            user_id      AS member_id,
            answer_value AS wants_to_continue_glp1,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY answered_at DESC
            )            AS rn
        FROM questionnaire_records
        WHERE question_id    = 'gV9Xu8RzF9hR'
          AND is_latest_answer = 1
    ) r
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- Reported medication (question knzp0ZppEBF4)
-- Member's self-reported prior GLP-1 medication name
-- ---------------------------------------------------------------------------
reported_med AS (
    SELECT member_id, reported_medication_name
    FROM (
        SELECT
            user_id      AS member_id,
            answer_text  AS reported_medication_name,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY answered_at DESC
            )            AS rn
        FROM questionnaire_records
        WHERE question_id    = 'knzp0ZppEBF4'
          AND is_latest_answer = 1
    ) r
    WHERE rn = 1
),

-- ---------------------------------------------------------------------------
-- WM prescriptions since 2026-01-01
-- therapy_type = 'WM' scopes to weight management medications only
-- ---------------------------------------------------------------------------
wm_rx_2026 AS (
    SELECT
        p.patient_user_id                                   AS member_id,
        1                                                   AS has_wm_rx_since_2026,
        MIN(DATE(p.prescribed_at))                          AS first_wm_rx_date,
        MAX(DATE(p.prescribed_at))                          AS latest_wm_rx_date,
        MAX(p.prescribed_at)                                AS latest_wm_prescribed_at,
        MAX(m.name)                                         AS latest_wm_rx_drug_name
    FROM prescriptions p
    JOIN medication_dosage_ndcs mdn ON mdn.ndc               = p.prescribed_ndc
    JOIN medication_dosages md      ON md.id                  = mdn.medication_dosage_id
    JOIN medications m              ON m.id                   = md.medication_id
    WHERE m.therapy_type       = 'WM'
      AND DATE(p.prescribed_at) >= '2026-01-01'
    GROUP BY p.patient_user_id
),

-- ---------------------------------------------------------------------------
-- First ever GLP-1 prescription (any date)
-- Used for drug switching classification
-- ---------------------------------------------------------------------------
first_glp1_rx AS (
    SELECT
        p.patient_user_id                                   AS member_id,
        MIN(p.prescribed_at)                                AS first_glp1_rx_date,
        MAX(CASE WHEN rn = 1 THEN m.name END)               AS first_glp1_rx_drug_name
    FROM (
        SELECT
            p2.patient_user_id,
            p2.prescribed_at,
            p2.prescribed_ndc,
            ROW_NUMBER() OVER (
                PARTITION BY p2.patient_user_id
                ORDER BY p2.prescribed_at ASC
            )                                               AS rn
        FROM prescriptions p2
    ) p
    JOIN medication_dosage_ndcs mdn ON mdn.ndc              = p.prescribed_ndc
    JOIN medication_dosages md      ON md.id                 = mdn.medication_dosage_id
    JOIN medications m              ON m.id                  = md.medication_id
    JOIN medication_drug_classes mdc ON mdc.medication_id   = m.id
    WHERE mdc.drug_class_name = 'GLP1'
    GROUP BY p.patient_user_id
),

-- ---------------------------------------------------------------------------
-- Zendesk continuation ticket data
-- Source of truth for continuation identification at the ticket level
-- Tags: glp1-continuation:ready-for-review | glp1-continuation:assistance-needed
-- Scoped to tickets created on or after 2026-01-01
-- ---------------------------------------------------------------------------
zendesk_continuation AS (
    SELECT
        zum.user_id                                         AS member_id,
        MIN(zt.created_at)                                  AS continuation_ticket_created_at,
        MIN(CASE
            WHEN ztt.tag = 'glp1-continuation:ready-for-review'
            THEN zt.created_at
        END)                                                AS ready_for_review_at,
        MIN(CASE
            WHEN ztt.tag = 'glp1-continuation:assistance-needed'
            THEN zt.created_at
        END)                                                AS assistance_needed_at,
        CASE
            WHEN MIN(CASE
                WHEN ztt.tag = 'glp1-continuation:ready-for-review'
                THEN zt.created_at
            END) IS NOT NULL
                THEN 'ready-for-review'
            WHEN MIN(CASE
                WHEN ztt.tag = 'glp1-continuation:assistance-needed'
                THEN zt.created_at
            END) IS NOT NULL
                THEN 'assistance-needed'
            ELSE NULL
        END                                                 AS ticket_type
    FROM zendesk_tickets zt
    JOIN zendesk_ticket_tags ztt   ON ztt.ticket_id        = zt.id
    JOIN zendesk_user_mappings zum ON zum.zendesk_user_id  = zt.requester_id
    WHERE ztt.tag IN (
        'glp1-continuation:ready-for-review',
        'glp1-continuation:assistance-needed'
    )
      AND zt.created_at >= '2026-01-01'
    GROUP BY zum.user_id
),

-- ---------------------------------------------------------------------------
-- Lab task: complete-initial-lab-order
-- COMPLETED or SKIPPED both count as lab-complete
-- ---------------------------------------------------------------------------
task_lab AS (
    SELECT
        user_id                                             AS member_id,
        MAX(CASE WHEN rn = 1 THEN status END)               AS lab_order_status,
        MAX(CASE WHEN rn = 1 THEN completed_at END)         AS lab_order_completed_at
    FROM (
        SELECT
            user_id,
            status,
            completed_at,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY updated_at DESC
            )                                               AS rn
        FROM tasks
        WHERE slug = 'complete-initial-lab-order'
    ) ranked
    GROUP BY user_id
),

-- ---------------------------------------------------------------------------
-- Questionnaire task: glp1-continuation-questionnaire
-- Only COMPLETED counts (SKIPPED does not apply here)
-- ---------------------------------------------------------------------------
task_quest AS (
    SELECT
        user_id                                             AS member_id,
        MAX(CASE WHEN rn = 1 THEN status END)               AS questionnaire_status,
        MAX(CASE WHEN rn = 1 THEN completed_at END)         AS questionnaire_completed_at
    FROM (
        SELECT
            user_id,
            status,
            completed_at,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY updated_at DESC
            )                                               AS rn
        FROM tasks
        WHERE slug = 'glp1-continuation-questionnaire'
    ) ranked
    GROUP BY user_id
)

-- =============================================================================
-- Final SELECT
-- =============================================================================
SELECT
    am.member_id,
    am.readable_id,
    am.primary_condition_group,
    e.employer_name,
    p.payer_name,
    am.subscription_start_date,
    am.days_enrolled,
    NOW()                                                   AS data_as_of,

    -- Continuation flags
    COALESCE(pu.past_glp1_use, 0)                           AS is_continuation_member,
    CASE
        WHEN COALESCE(pu.past_glp1_use, 0) = 1 THEN 'Continuation'
        ELSE 'New to Med'
    END                                                     AS member_type,
    COALESCE(ca.wants_to_continue_glp1, 0)                  AS wants_to_continue_glp1,
    rm.reported_medication_name,

    -- Prescription fields
    COALESCE(wm.has_wm_rx_since_2026, 0)                    AS has_wm_rx_since_2026,
    wm.first_wm_rx_date,
    wm.latest_wm_rx_date,
    wm.latest_wm_prescribed_at,
    wm.latest_wm_rx_drug_name,
    frx.first_glp1_rx_date,
    frx.first_glp1_rx_drug_name,

    -- Zendesk ticket fields
    zc.continuation_ticket_created_at,
    zc.ready_for_review_at,
    zc.assistance_needed_at,
    zc.ticket_type,

    -- Task status fields
    tq.questionnaire_status,
    tq.questionnaire_completed_at,
    tl.lab_order_status,
    tl.lab_order_completed_at,
    CASE WHEN tq.questionnaire_status = 'COMPLETED'                     THEN 1 ELSE 0 END AS quest_completed,
    CASE WHEN tl.lab_order_status IN ('COMPLETED', 'SKIPPED')           THEN 1 ELSE 0 END AS lab_completed,

    -- Both tasks complete timestamp (later of the two)
    CASE
        WHEN tl.lab_order_status IN ('COMPLETED', 'SKIPPED')
         AND tq.questionnaire_status = 'COMPLETED'
        THEN GREATEST(
            COALESCE(tl.lab_order_completed_at, '1900-01-01'),
            COALESCE(tq.questionnaire_completed_at, '1900-01-01')
        )
        ELSE NULL
    END                                                     AS both_tasks_completed_at,

    -- Funnel bucket (drives Pending Task Completion section)
    CASE
        WHEN COALESCE(wm.has_wm_rx_since_2026, 0) = 1
            THEN 'Has WM Rx'
        WHEN tl.lab_order_status IN ('COMPLETED', 'SKIPPED')
         AND tq.questionnaire_status = 'COMPLETED'
            THEN 'Ready for Rx'
        WHEN tl.lab_order_status IN ('COMPLETED', 'SKIPPED')
         AND COALESCE(tq.questionnaire_status, '') != 'COMPLETED'
            THEN 'Missing Questionnaire'
        WHEN COALESCE(tl.lab_order_status, '') NOT IN ('COMPLETED', 'SKIPPED')
         AND tq.questionnaire_status = 'COMPLETED'
            THEN 'Missing Lab'
        ELSE 'Missing Both'
    END                                                     AS member_category,

    -- Clock start: later of ticket creation and questionnaire completion
    -- Represents when the member was truly ready for clinical review
    CASE
        WHEN zc.continuation_ticket_created_at IS NOT NULL
         AND tq.questionnaire_completed_at IS NOT NULL
        THEN GREATEST(
            zc.continuation_ticket_created_at,
            tq.questionnaire_completed_at
        )
        WHEN zc.continuation_ticket_created_at IS NOT NULL
        THEN zc.continuation_ticket_created_at
        ELSE NULL
    END                                                     AS clock_start,

    -- Flag: questionnaire was completed after ticket, so used as clock start
    CASE
        WHEN zc.continuation_ticket_created_at IS NOT NULL
         AND tq.questionnaire_completed_at IS NOT NULL
         AND zc.continuation_ticket_created_at < tq.questionnaire_completed_at
            THEN 1
        ELSE 0
    END                                                     AS questionnaire_used_as_clock_start,

    -- Flag: member has both a ticket and questionnaire (usable clock start exists)
    CASE
        WHEN zc.continuation_ticket_created_at IS NOT NULL
         AND tq.questionnaire_completed_at IS NOT NULL
            THEN 1
        ELSE 0
    END                                                     AS has_usable_clock_start,

    -- Lag metrics
    -- days_clock_start_to_rx: powers "Ready for Review to Rx"
    CASE
        WHEN wm.first_wm_rx_date IS NOT NULL
         AND zc.continuation_ticket_created_at IS NOT NULL
         AND tq.questionnaire_completed_at IS NOT NULL
        THEN DATEDIFF(
            wm.first_wm_rx_date,
            DATE(GREATEST(
                zc.continuation_ticket_created_at,
                tq.questionnaire_completed_at
            ))
        )
        ELSE NULL
    END                                                     AS days_clock_start_to_rx,

    -- approx_business_days_to_rx: approximates biz days via (5/7) multiplier
    CASE
        WHEN wm.latest_wm_prescribed_at IS NOT NULL
         AND zc.continuation_ticket_created_at IS NOT NULL
         AND tq.questionnaire_completed_at IS NOT NULL
        THEN ROUND(
            TIMESTAMPDIFF(HOUR,
                GREATEST(
                    zc.continuation_ticket_created_at,
                    tq.questionnaire_completed_at
                ),
                wm.latest_wm_prescribed_at
            ) / 24.0 * (5.0 / 7.0),
            1
        )
        ELSE NULL
    END                                                     AS approx_business_days_to_rx,

    -- days_since_enrollment: powers "Avg Wait Without Rx" denominator
    DATEDIFF(CURDATE(), am.subscription_start_date)         AS days_since_enrollment,

    -- days_enrollment_to_rx: powers "Enrollment to Rx" lag metric
    CASE
        WHEN wm.first_wm_rx_date IS NOT NULL
        THEN DATEDIFF(wm.first_wm_rx_date, am.subscription_start_date)
        ELSE NULL
    END                                                     AS days_enrollment_to_rx,

    -- days_tasks_to_rx: powers "Longest Lag" metric
    CASE
        WHEN wm.latest_wm_prescribed_at IS NOT NULL
         AND tl.lab_order_status IN ('COMPLETED', 'SKIPPED')
         AND tq.questionnaire_status = 'COMPLETED'
        THEN DATEDIFF(
            DATE(wm.latest_wm_prescribed_at),
            DATE(GREATEST(
                COALESCE(tl.lab_order_completed_at, '1900-01-01'),
                COALESCE(tq.questionnaire_completed_at, '1900-01-01')
            ))
        )
        ELSE NULL
    END                                                     AS days_tasks_to_rx,

    -- days_waiting_since_ready: for members with no Rx but both tasks complete
    -- powers "Avg Wait Without Rx"
    CASE
        WHEN COALESCE(wm.has_wm_rx_since_2026, 0) = 0
         AND tl.lab_order_status IN ('COMPLETED', 'SKIPPED')
         AND tq.questionnaire_status = 'COMPLETED'
        THEN DATEDIFF(CURDATE(), DATE(GREATEST(
            COALESCE(tl.lab_order_completed_at, '1900-01-01'),
            COALESCE(tq.questionnaire_completed_at, '1900-01-01')
        )))
        ELSE NULL
    END                                                     AS days_waiting_since_ready

FROM active_members am
LEFT JOIN employer              e   ON e.member_id   = am.member_id
LEFT JOIN payer                 p   ON p.member_id   = am.member_id
LEFT JOIN glp1_past_use         pu  ON pu.member_id  = am.member_id
LEFT JOIN continuation_answer   ca  ON ca.member_id  = am.member_id
LEFT JOIN reported_med          rm  ON rm.member_id  = am.member_id
LEFT JOIN wm_rx_2026            wm  ON wm.member_id  = am.member_id
LEFT JOIN first_glp1_rx         frx ON frx.member_id = am.member_id
LEFT JOIN zendesk_continuation  zc  ON zc.member_id  = am.member_id
LEFT JOIN task_lab              tl  ON tl.member_id  = am.member_id
LEFT JOIN task_quest            tq  ON tq.member_id  = am.member_id

ORDER BY am.subscription_start_date DESC;


-- =============================================================================
-- QUERY 2: continuation answers (secondary source)
-- Description : Doses-remaining questionnaire answers for continuation members.
--               Joined to Query 1 on readable_id in Tableau.
-- Grain       : One row per active continuation member with <= 30 doses left
-- Scope       : State of Georgia employer only
-- Question ID : UUeznpkuACcR
-- =============================================================================

SELECT
    BIN_TO_UUID(qr.user_id)                                AS readable_id,
    qr.user_id                                             AS member_id,
    CAST(qr.answer_value AS DECIMAL)                       AS doses_left,
    qr.answered_at,
    pe.name                                                AS employer_name,
    MIN(s.start_date)                                      AS subscription_start_date,
    DATEDIFF(qr.answered_at, MIN(s.start_date))            AS days_after_enrollment
FROM questionnaire_records qr
JOIN subscriptions s
    ON  s.user_id           = qr.user_id
    AND s.status            = 'ACTIVE'
    AND s.cancellation_date IS NULL
JOIN partner_employers pe
    ON  pe.user_id = qr.user_id
    AND pe.name    = 'State of Georgia'
-- Inner join ensures only confirmed continuation members are included
JOIN (
    SELECT DISTINCT user_id
    FROM questionnaire_records
    WHERE question_id    = 'A8z9j98E0sxR'  -- past GLP-1 use
      AND answer_value   = 1
      AND is_latest_answer = 1
) continuation ON continuation.user_id = qr.user_id
WHERE qr.question_id     = 'UUeznpkuACcR'  -- doses remaining
  AND qr.is_latest_answer = 1
  AND CAST(qr.answer_value AS DECIMAL) <= 30
GROUP BY
    qr.user_id,
    qr.answer_value,
    qr.answered_at,
    pe.name;
