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
),

leads_tab AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        date_trunc('day', visit_date) AS visit_date,
        count(lead_id) AS leads_count,
        count(
            CASE WHEN closing_reason = 'Успешная продажа' THEN 1 END
        ) AS purchases_count,
        sum(coalesce(amount, 0)) AS revenue
    FROM lpt
    GROUP BY
        date_trunc('day', visit_date),
        utm_source,
        utm_medium,
        utm_campaign
    ORDER BY visit_date ASC
),

ss_tab AS (
    SELECT
        sessions.source AS utm_source,
        sessions.medium AS utm_medium,
        sessions.campaign AS utm_campaign,
        date_trunc('day', sessions.visit_date) AS visit_date,
        count(sessions.visitor_id) AS visitors_count
    FROM sessions
    GROUP BY
        date_trunc('day', sessions.visit_date),
        utm_source,
        utm_medium,
        utm_campaign
    ORDER BY visit_date ASC
),

ads_tab AS (
    SELECT
        date(campaign_date) AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) AS cost
    FROM vk_ads
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        date(campaign_date) AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) AS cost
    FROM ya_ads
    GROUP BY 1, 2, 3, 4
),

-- Собираем все данные в одну таблицу
refined_data AS (
    select
        to_char(ss_tab.visit_date, 'YYYY-MM-DD') AS visit_date
        ss_tab.visitors_count,
        ss_tab.utm_source,
        ss_tab.utm_medium,
        ss_tab.utm_campaign,
        ads_tab.cost AS total_cost,
        leads_count,
        purchases_count,
        revenue
        
    FROM ss_tab
    FULL OUTER JOIN ads_tab
        ON
            ss_tab.utm_source = ads_tab.utm_source
            AND ss_tab.utm_medium = ads_tab.utm_medium
            AND ss_tab.utm_campaign = ads_tab.utm_campaign
            AND ss_tab.visit_date = ads_tab.campaign_date
    FULL OUTER JOIN leads_tab
        ON
            ss_tab.utm_source = leads_tab.utm_source
            AND ss_tab.utm_medium = leads_tab.utm_medium
            AND ss_tab.utm_campaign = leads_tab.utm_campaign
            AND ss_tab.visit_date = leads_tab.visit_date
    ORDER BY
        revenue DESC NULLS LAST,
        visit_date ASC,
        visitors_count DESC,
        utm_source ASC,
        utm_medium ASC,
        utm_campaign ASC
    LIMIT 15
)

SELECT * FROM refined_data;