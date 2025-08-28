select
    DATE_TRUNC(month, timestamp) as month,
    movie_id,
    sum(ticket_amount) as tickets_sold,
    sum(transaction_total) as revenue,
    'nj_001' as location
from {{ source("silverscreen", "nj_001") }}
GROUP BY month, movie_id
