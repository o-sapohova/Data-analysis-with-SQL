/* Розрахунок кількості створених акаунтів в розрізі заданих параметрів */
WITH
  account AS (
  SELECT
    s.date,
    sp.country,
    ac.send_interval,
    ac.is_verified,
    ac.is_unsubscribed,
    COUNT(acs.ga_session_id) AS account_cnt
  FROM
    `data-analytics-mate.DA.account_session` acs
  JOIN
    `data-analytics-mate.DA.account` ac
  ON
    acs.account_id = ac.id
  JOIN
    `data-analytics-mate.DA.session` s
  ON
    acs.ga_session_id = s.ga_session_id
  JOIN
    `data-analytics-mate.DA.session_params` sp
  ON
    acs.ga_session_id = sp.ga_session_id
  GROUP BY
    s.date,
    sp.country,
    ac.send_interval,
    ac.is_verified,
    ac.is_unsubscribed ),
  /* Розрахунок кількості sent_msg, open_msg, visit_msg в розрізі заданих параметрів */
  message AS (
  SELECT
    DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
    sp.country,
    ac.send_interval,
    ac.is_verified,
    ac.is_unsubscribed,
    COUNT(DISTINCT es.id_message) AS sent_msg,
    COUNT(DISTINCT eo.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM
    `data-analytics-mate.DA.account_session` acs
  JOIN
    `data-analytics-mate.DA.account` ac
  ON
    acs.account_id = ac.id
  JOIN
    `data-analytics-mate.DA.session` s
  ON
    acs.ga_session_id = s.ga_session_id
  JOIN
    `data-analytics-mate.DA.session_params` sp
  ON
    acs.ga_session_id = sp.ga_session_id
  JOIN
    `data-analytics-mate.DA.email_sent` es
  ON
    acs.account_id = es.id_account
  LEFT JOIN
    `data-analytics-mate.DA.email_open` eo
  ON
    es.id_message = eo.id_message
  LEFT JOIN
    `data-analytics-mate.DA.email_visit` ev
  ON
    es.id_message = ev.id_message
  GROUP BY
    date,
    sp.country,
    ac.send_interval,
    ac.is_verified,
    ac.is_unsubscribed ),
  /* Об'єднання account та message*/
  union_data AS (
  SELECT
    date AS date,
    account.country AS country,
    send_interval AS send_interval,
    is_verified AS is_verified,
    is_unsubscribed AS is_unsubscribed,
    account_cnt AS account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM
    account
  UNION ALL
  SELECT
    date AS date,
    message.country AS country,
    send_interval AS send_interval,
    is_verified AS is_verified,
    is_unsubscribed AS is_unsubscribed,
    0 AS account_cnt,
    sent_msg AS sent_msg,
    open_msg AS open_msg,
    visit_msg AS visit_msg
  FROM
    message ),
  /* Додавння до union_data розрахунку кількості створених акаунтів та кількості відправлених листів по країнам загалом */      
  union_data_with_total_cnt AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg,
    SUM(SUM(account_cnt)) OVER (PARTITION BY country) AS total_country_account_cnt,
    SUM(SUM(sent_msg)) OVER (PARTITION BY country) AS total_country_sent_cnt
  FROM
    union_data
  GROUP BY
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed )
/* Обчислення рейтингів країн за кількістю створених підписників та за кількістю відправлених листів в цілому по країні, обмеження результату вибірки */
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  sent_msg,
  open_msg,
  visit_msg,
  total_country_account_cnt,
  total_country_sent_cnt,
  DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
  DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
FROM
  union_data_with_total_cnt
QUALIFY
  DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) <= 10
  OR DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) <= 10
ORDER BY
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed;
