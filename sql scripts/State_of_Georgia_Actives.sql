select @date := '2026-02-04';

select
    bus.user_id,
    eui.external_identifier,
    ud.first_name,
    ud.last_name,
    ud.date_of_birth
from billable_user_statuses bus
join nineamdwh_restricted.user_details ud on ud.user_id = bus.user_id
join external_user_identifiers eui on eui.user_id = bus.user_id and eui.type = 'state-of-georgia-employee-id'
where bus.partner = 'State of Georgia'
    and bus.date = @date
    and bus.subscription_status = 'ACTIVE'
    and bus.user_status != 'partner-ineligible'
;