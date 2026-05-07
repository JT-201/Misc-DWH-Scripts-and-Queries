SELECT
      BIN2UUID(t.user_id),
      IF(c.id IS NOT NULL, 'true', 'false') AS has_fillex_rx
  FROM temp_user_ids t
  LEFT JOIN shbp_georgia_cvs_rx_claims c
      ON c.id = (
          SELECT c2.id
          FROM shbp_georgia_cvs_rx_claims c2
          JOIN partner_eligibility_checks ec
              ON ec.user_id = t.user_id
             AND ec.payer_id = UUID_TO_BIN('3a437954-5990-4359-86e4-a4d877a012f6')
             AND ec.status = 'ELIGIBLE'
          JOIN partner_eligibility_specific_user_data esd
              ON esd.eligibility_check_id = ec.id
             AND esd.key = 'employeeId'
             AND esd.value = c2.alternate_id
          JOIN prescription_prescriptions p
              ON p.patient_user_id = t.user_id
             AND ndc_as_code(p.prescribed_ndc) = ndc_as_code(c2.product_id)
             AND c2.date_filled > p.created_at
          ORDER BY c2.date_filled DESC
          LIMIT 1
      )
      ORDER BY t.user_id ASC;
