select
    movie_id,
    month,
    lower(location_id) as location,  -- lowercase for later joing
    max(total_invoice_sum) as rental_cost -- handle duplicated groups properly

from {{ source("silverscreen", "invoices") }}
group by movie_id, month, location_id
