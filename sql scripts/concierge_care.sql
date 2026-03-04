-- Get counts of who answered yes/no to concierge care
SELECT 
    qr.question_title,            
    qr.question_id,                
    qr.answer_text,
    COUNT(DISTINCT qr.user_id) as total_users
FROM questionnaire_records qr
JOIN users u ON qr.user_id = u.id                         
JOIN partner_employers pe ON u.id = pe.user_id 
WHERE qr.question_title LIKE '%seamless concierge care%'
  AND qr.is_latest_answer = 1
  AND pe.name = 'Thermo Fisher'
  AND answered_at >= '2026-01-18' and answered_at < '2026-02-18' 
  and u.status = 'ACTIVE' 
GROUP BY 
    qr.question_title, 
    qr.question_id, 
    qr.answer_text;
