-- visitor_id с последней даты с неорганики
WITH last_paid AS (
    SELECT
        visitor_id,
        max(visit_date) AS max_paid_date
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY 1
),
-- подтягиваем их метки из начальной таблицы
-- крепим данные лидов
-- группируем для итоговой таблицы
leads_tab AS (
    SELECT
        date_trunc('day', max_paid_date) AS visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        count(lead_id) AS leads_count,
        count(distinct s.visitor_id) AS visitors_count,
        count(
            CASE
                WHEN
                    closing_reason = 'Успешная продажа' OR status_id = 142
                    THEN leads.visitor_id
            END
        ) AS purchases_count,
        sum(coalesce(amount, 0)) AS revenue
    FROM sessions AS s
    LEFT JOIN last_paid
        ON
            s.visitor_id = last_paid.visitor_id
            AND s.visit_date = last_paid.max_paid_date
    LEFT JOIN leads
        ON
            last_paid.visitor_id = leads.visitor_id
            AND leads.created_at >= visit_date
    WHERE source IS NOT null
    GROUP BY
        1, 2, 3, 4
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
refined_data AS (
    select
        date(leads_tab.visit_date) AS visit_date,
        leads_tab.visitors_count,
        leads_tab.utm_source,
        leads_tab.utm_medium,
        leads_tab.utm_campaign,
        ads_tab.cost AS total_cost,
        leads_count,
        purchases_count,
        revenue  
    FROM leads_tab
    FULL OUTER JOIN ads_tab
        ON
            leads_tab.utm_source = ads_tab.utm_source
            AND leads_tab.utm_medium = ads_tab.utm_medium
            AND leads_tab.utm_campaign = ads_tab.utm_campaign
            AND leads_tab.visit_date = ads_tab.campaign_date
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