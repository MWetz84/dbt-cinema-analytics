select
    date_trunc(month, date) as month,
    movie_id,
    sum(ticket_amount) as tickets_sold,
    sum(total_earned) as revenue,
    'nj_002' as location

from {{ source("dbt_cinema_analytics", "nj_002") }}
GROUP BY
    month, movie_id
