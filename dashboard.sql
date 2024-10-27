WITH last_paid AS (
    SELECT
        visitor_id,
        max(visit_date) AS max_paid_date
    FROM sessions
    WHERE medium != 'organic'
    GROUP BY 1
),
-- Дата последнего захода. Потом использую чтобы можно было и органику тоже отслеживать. Где не нужно - уберу.
last_date AS (
    SELECT
        visitor_id,
        max(visit_date) AS max_date
    FROM sessions
    GROUP BY 1
),
-- Промежуточная таблица, подбор дат последнего захода к visitor_id.
-- Последняя дата - дата последнего захода с неорганики. Если такого нет, то последнего с органики.
max_visit_date AS (
    SELECT
        s.visitor_id,
        source,
        medium,
        campaign,
        s.visit_date,
        (coalesce (max_paid_date, max_date)) AS max_v_date
    FROM sessions AS s
    LEFT JOIN last_paid USING (visitor_id)
LEFT JOIN last_date USING (visitor_id)
),
-- Отфильтруем, оставим по 1 записи на каждого visitor_id с датой последнего захода и сопутствующими метками.
last_visit AS (
SELECT *
FROM max_visit_date
WHERE max_v_date = visit_date
ORDER BY visitor_id
),
-- Структура лидов по меткам:
-- Теперь крепим к лидам записи visitor_id с метками и датой последнего захода
leads_structure AS (
SELECT
    leads.visitor_id,
    lead_id,
    amount,
    created_at,
    closing_reason,
    status_id,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    max_v_date,
    (created_at - max_v_date) AS close_time
FROM leads LEFT JOIN last_visit ON leads.visitor_id = last_visit.visitor_id
ORDER BY lead_id
),
--Отдельная табличка чтобы посмотеть какими темпами закрываются лиды, close_time
ct AS (
SELECT
*,
row_number() OVER (ORDER BY close_time ASC) AS rn
FROM leads_structure
WHERE close_time IS NOT null
),
-- Отберём запись, соответствующую уровню 90 из 100 
ct2 AS (
SELECT
close_time,
rn
FROM ct WHERE rn = (SELECT (max(rn) * 90 / 100) AS p FROM ct)
),
-- Обобщаем данные посещений сайта по датам и меткам
ss_tab AS (
SELECT
sessions.source AS utm_source,
sessions.medium AS utm_medium,
sessions.campaign AS utm_campaign,
date_trunc('day', sessions.visit_date) AS visit_date,
count(sessions.visitor_id) AS visitors_count
FROM sessions
GROUP BY
date_trunc('day', sessions.visit_date), utm_source, utm_medium, utm_campaign
ORDER BY visit_date ASC
),
--Обобщаем лидов по датам и меткам. В основе - таблица сессий, к ней крепим данные лидов. 
--Собираем итоги visitors_count, leads_count, purchases_count, revenue с разбивкой по датам и меткам.
leads_tab AS (
SELECT
l_a.utm_source,
l_a.utm_medium,
l_a.utm_campaign,
date_trunc('day', sessions.visit_date) AS visit_date,
count(sessions.visitor_id) AS visitors_count,
count(lead_id) AS leads_count,
count(
    CASE WHEN closing_reason = 'Успешная продажа' THEN 1 END
) AS purchases_count,
sum(coalesce(amount, 0)) AS revenue
FROM sessions
LEFT JOIN
leads_structure AS l_a
ON sessions.visitor_id = l_a.visitor_id AND sessions.visit_date = l_a.max_v_date
GROUP BY
date_trunc('day', sessions.visit_date),
l_a.utm_source,
l_a.utm_medium,
l_a.utm_campaign
ORDER BY visit_date ASC
),
-- Обобщаем расходы на рекламные кампании по датам и меткам
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
refined_data AS (
SELECT
ss_tab.visit_date AS ss_visit_date,
ss_tab.utm_source AS ss_source,
ss_tab.utm_medium AS ss_medium,
ss_tab.utm_campaign AS ss_campaign,
ss_tab.visitors_count AS ss_visitors,
leads_tab.visit_date AS leads_visit_date,
leads_tab.utm_source AS l_utm_source,
leads_tab.utm_medium AS l_utm_medium,
leads_tab.utm_campaign AS l_utm_campaign,
leads_count,
purchases_count,
revenue,
ads_tab.cost,
ads_tab.campaign_date AS ads_campaign_date,
ads_tab.utm_source AS ads_utm_source,
ads_tab.utm_medium AS ads_utm_medium,
ads_tab.utm_campaign AS ads_utm_campaign
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
ORDER BY ss_tab.visit_date ASC, ads_tab.campaign_date DESC NULLS LAST
),
-- таблица для расчёта корреляции
corr_data AS (
SELECT
ss_visit_date,
sum(ss_visitors) AS sum_v,
sum(coalesce(cost, 0))
FROM refined_data
GROUP BY ss_visit_date
ORDER BY ss_visit_date
),
cr AS (
SELECT
corr(corr_data.sum_v, corr_data.sum) AS correlation_0,
corr(cd.sum_v, corr_data.sum) AS correlation_1
FROM corr_data
LEFT JOIN
corr_data AS cd
ON corr_data.ss_visit_date = (cd.ss_visit_date + interval '1 day')
),
--Агрегируем данные по метрикам, опуская даты для итоговых расчётов в дашборде
ads_metrics AS (
SELECT
ss_source,
ss_medium,
ss_campaign,
sum(coalesce(ss_visitors, 0)) AS visitors_count,
sum(coalesce(leads_count, 0)) AS leads_count,
sum(coalesce(purchases_count, 0)) AS purchases_count,
sum(coalesce(revenue, 0)) AS revenue_sum,
sum(coalesce(cost, 0)) AS total_cost_sum
FROM refined_data
GROUP BY
ss_source, ss_medium, ss_campaign
ORDER BY leads_count DESC
),
dashboard AS (
SELECT
ss_source AS source,
ss_medium AS medium,
ss_campaign AS campaign,
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
FROM ads_metrics
WHERE total_cost_sum != 0
ORDER BY roi DESC NULLS LAST
)
SELECT * FROM dashboard