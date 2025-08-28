    -- create new structure + filter by product_type 'ticket' + rename details to 'movie_id'
    select
        DATE_TRUNC(month, timestamp) as month,
        details as movie_id,
        SUM(amount) as tickets_sold,
        SUM(total_value) as revenue,
        'nj_003' as location

    from {{ source('silverscreen', 'nj_003') }}
    WHERE product_type = 'ticket'
    GROUP BY
        month, movie_id