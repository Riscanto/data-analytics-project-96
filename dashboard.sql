--DASHBOARD
WITH last_paid AS (
    SELECT
        visitor_id,
        max(visit_date) AS max_paid_date
    FROM sessions
    WHERE medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY 1
),
leads_tab AS (
    select
        date(max_paid_date) AS visit_date,
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
    FROM last_paid
    INNER JOIN sessions s
        ON
            s.visitor_id = last_paid.visitor_id
            AND s.visit_date = last_paid.max_paid_date
    LEFT JOIN leads
        ON
            last_paid.visitor_id = leads.visitor_id
            AND leads.created_at >= last_paid.max_paid_date
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
-- Собираем все данные в одну таблицу, дальше будем только агрегировать в несколько шагов
--Здесь у нас группированные данные, включая группировку по датам, для анализа.
refined_data AS (
SELECT
leads_tab.visit_date AS visit_date,
leads_tab.utm_source AS utm_source,
leads_tab.utm_medium AS utm_medium,
leads_tab.utm_campaign AS utm_campaign,
visitors_count,
leads_count,
purchases_count,
revenue,
ads_tab.cost
FROM leads_tab
LEFT JOIN ads_tab
ON
    leads_tab.utm_source = ads_tab.utm_source
    AND leads_tab.utm_medium = ads_tab.utm_medium
    AND leads_tab.utm_campaign = ads_tab.utm_campaign
    AND leads_tab.visit_date = ads_tab.campaign_date
    ORDER BY
        revenue DESC NULLS LAST,
        1 ASC,
        5 DESC,
        2 ASC,
        3 ASC,
        4 ASC
),
--Агрегируем данные по метрикам, опуская даты для итоговых расчётов в дашборде
a_metrics AS (
SELECT
utm_source source,
utm_medium medium,
utm_campaign campaign,
sum(coalesce(visitors_count, 0)) AS visitors_count,
sum(coalesce(leads_count, 0)) AS leads_count,
sum(coalesce(purchases_count, 0)) AS purchases_count,
sum(coalesce(revenue, 0)) AS revenue_sum,
sum(coalesce(cost, 0)) AS total_cost_sum
FROM refined_data
GROUP BY
1, 2, 3
ORDER BY leads_count DESC
),
dashboard AS (
SELECT
source,
medium,
campaign,
visitors_count,
leads_count,
purchases_count,
revenue_sum,
total_cost_sum,
(
    CASE WHEN visitors_count != 0 THEN (leads_count / visitors_count) * 100 END
) AS visit_to_lead,
(
    CASE WHEN leads_count != 0 THEN (purchases_count / leads_count) * 100 END
) AS lead_to_purchase,
(
    CASE WHEN visitors_count != 0 THEN (total_cost_sum / visitors_count) END
) AS cpu,
(
    CASE WHEN leads_count != 0 THEN (total_cost_sum / leads_count) END
) AS cpl,
(
    CASE WHEN purchases_count != 0 THEN (total_cost_sum / purchases_count) END
) AS cppu,
(
    CASE
        WHEN
            total_cost_sum != 0
            THEN ((revenue_sum - total_cost_sum) / (total_cost_sum))
        END
    ) AS roi
FROM a_metrics
ORDER BY roi DESC NULLS LAST
),
leads_structure AS (
SELECT
    leads.visitor_id,
    max_paid_date visit_date,
    lead_id,
    created_at,
    (created_at - max_paid_date) AS close_time,
    row_number() OVER (ORDER BY 8 asc) AS rn
FROM leads LEFT JOIN last_paid ON leads.visitor_id = last_paid.visitor_id
WHERE max_paid_date IS NOT null and max_paid_date <= created_at
ORDER BY lead_id
),
--Отдельная табличка чтобы посмотеть какими темпами закрываются лиды, close_time
-- Отберём запись, соответствующую уровню 90 из 100 
ct AS (
SELECT
close_time,
rn
FROM leads_structure WHERE rn = (SELECT (max(rn) * 90 / 100) AS p FROM leads_structure)
),
organic_visits as (
select 
date(visit_date) AS visit_date,
        count(distinct s.visitor_id) AS visitors_count
        from sessions s
        where medium = 'organic'
        group by 1
        ),
-- таблица для расчёта корреляции
corr_data AS (
select
date(visit_date) AS visit_date,
        visitors_count,
sum(cost) as cost
FROM organic_visits full join ads_tab on ads_tab.campaign_date = organic_visits.visit_date
GROUP BY 1, 2
ORDER BY 1
),
cr AS (
SELECT
corr(corr_data.visitors_count, corr_data.cost) AS correlation_0,
corr(cd.visitors_count, corr_data.cost) AS correlation_1
FROM corr_data
LEFT JOIN
corr_data AS cd
ON corr_data.visit_date = (cd.visit_date + interval '1 day')
)
SELECT * FROM dashboard
;
