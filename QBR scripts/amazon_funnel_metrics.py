"""
amazon_funnel_metrics.py

Purpose:
Pull Amazon Q1 2026 funnel, behavioral health, and eligibility check metrics.

Notes:
- Funnel logic is intentionally non-strict.
- Registration uses `Registration Completed` event with fallback to `users.created_at`.
- This avoids undercounting users with missing registration events or historical activity.
"""

# Reporting window
START_DATE = "2026-01-01"
END_DATE = "2026-05-01"
EMPLOYER_NAME = "Amazon"
AMAZON_PAYER_ID = "ec58accf-ddf4-4825-ac12-a2f56fd5a51c"


AMAZON_FUNNEL_QUERY = f"""
with amazon_users as (
    select u.id as user_id,
           min(u.created_at) as created_at
    from partner_employers pe
    join users u on u.id = pe.user_id
    where pe.name = '{EMPLOYER_NAME}'
    group by u.id
),

reg_completed as (
    select au.user_id,
           coalesce(min(ae.created_at), min(au.created_at)) as reg_completed_at
    from amazon_users au
    left join analytics_events ae
      on ae.user_id = au.user_id
     and ae.event_name = 'Registration Completed'
    group by au.user_id
),

users_answered_med_questionnaire as (
    select au.user_id,
           min(qr.answered_at) as first_answered_at,
           max(qr.answered_at) as last_answered_at
    from amazon_users au
    join questionnaire_records qr
      on qr.user_id = au.user_id
    where qr.questionnaire_id = 'wRvs6BNP'
    group by au.user_id
),

latest_med_eligibility as (
    select user_id,
           "timestamp",
           eligibility
    from (
        select al.user_id,
               al."timestamp",
               alpf.value::varchar as eligibility,
               row_number() over (
                   partition by al.user_id
                   order by al."timestamp" desc
               ) as rn
        from amazon_users au
        join audit_logs al
          on al.user_id = au.user_id
        join audit_log_payload_fields alpf
          on alpf.audit_log_id = al.id
         and alpf."key" = 'eligibility'
        where al.event_name = 'program.generic.medical_eligibility_determined'
    ) sub
    where rn = 1
),

subscription_completed as (
    select au.user_id,
           min(s.start_date) as subscription_started_at
    from amazon_users au
    join subscriptions s
      on s.user_id = au.user_id
     and s.start_date is not null
    group by au.user_id
),

first_weight_module_completed as (
    select au.user_id,
           min(t.started_at) as first_module_started_at,
           min(t.completed_at) as first_module_completed_at
    from amazon_users au
    join tasks t
      on t.user_id = au.user_id
    where t.program = 'path-to-healthy-weight'
      and t."group" = 'module01'
      and t.completed_at is not null
    group by au.user_id
)

select
    count(distinct au.user_id) as registered,
    count(distinct uamq.user_id) as answered_med_questionnaire,
    count(distinct case when lme.eligibility = 'ELIGIBLE' then lme.user_id end) as med_eligible,
    count(distinct case when lme.eligibility = 'INELIGIBLE' then lme.user_id end) as med_ineligible,
    count(distinct sc.user_id) as subscription_completed,
    count(distinct fwmc.user_id) as first_module_completed
from amazon_users au
left join reg_completed rc
  on rc.user_id = au.user_id
left join users_answered_med_questionnaire uamq
  on uamq.user_id = au.user_id
left join latest_med_eligibility lme
  on lme.user_id = au.user_id
left join subscription_completed sc
  on sc.user_id = au.user_id
left join first_weight_module_completed fwmc
  on fwmc.user_id = au.user_id
where rc.reg_completed_at between '{START_DATE}' and '{END_DATE}';
"""


BHS_SCORE_DISTRIBUTION_QUERY = f"""
select aep."value" as score_range,
       count(distinct ae.user_id) as distinct_users
from analytics_events ae
join analytics_event_properties aep
  on aep.analytics_event_id = ae.id
 and aep.key = 'Score range'
join partner_employers pe
  on pe.user_id = ae.user_id
join subscriptions s
  on s.user_id = ae.user_id
where pe.name = '{EMPLOYER_NAME}'
  and ae.event_name = 'BHS score'
  and s.status = 'ACTIVE'
  and ae.created_at between '{START_DATE}' and '{END_DATE}'
group by aep."value"
order by aep."value";
"""


ELIGIBILITY_CHECKS_QUERY = f"""
select count(distinct pec.user_id) as distinct_users_checked,
       count(distinct pec.id) as total_eligibility_checks
from partner_eligibility_checks pec
where pec.payer_id = UUID_TO_BIN('{AMAZON_PAYER_ID}')
  and pec.performed_at between '{START_DATE}' and '{END_DATE}';
"""


if __name__ == "__main__":
    print("Amazon Funnel Query:")
    print(AMAZON_FUNNEL_QUERY)

    print("\\nBHS Score Distribution Query:")
    print(BHS_SCORE_DISTRIBUTION_QUERY)

    print("\\nEligibility Checks Query:")
    print(ELIGIBILITY_CHECKS_QUERY)
