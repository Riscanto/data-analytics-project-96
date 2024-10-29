WITH last_paid AS (
    SELECT
        visitor_id,
        max(visit_date) AS max_paid_date
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY 1
),
last_visit AS (
    SELECT
        s.visitor_id,
        source,
        medium,
        campaign,
        s.visit_date,
        max_paid_date
    FROM sessions AS s
    LEFT JOIN last_paid ON s.visitor_id = last_paid.visitor_id
WHERE max_paid_date = s.visit_date
),
lpt AS (
SELECT
    leads.visitor_id,
    max_paid_date AS visit_date,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM leads LEFT JOIN last_visit ON leads.visitor_id = last_visit.visitor_id
WHERE source IS NOT null
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10
)
SELECT * FROM lpt;