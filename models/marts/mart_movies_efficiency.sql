WITH joined_data AS (
    SELECT
        rev.movie_id,
        moc.movie_title,
        moc.genre,
        moc.studio,
        rev.month,
        rev.location,
        COALESCE(inv.rental_cost, 0) AS rental_cost, -- handle null values in costs
        rev.tickets_sold,
        rev.revenue
    FROM {{ ref("int_revenue") }} rev
    LEFT JOIN {{ ref("stg_invoice") }} inv
        ON rev.movie_id = inv.movie_id
        AND rev.location = inv.location
        AND rev.month = inv.month
    LEFT JOIN {{ ref("stg_movie_catalogue") }} moc
        ON rev.movie_id = moc.movie_id
),

kpi_enrichment AS (
    SELECT
        joined_data.*,
        (revenue - rental_cost) AS profit,
        (revenue - rental_cost) / NULLIF(rental_cost, 0) AS roi
    FROM joined_data
)

SELECT
    *
FROM kpi_enrichment
ORDER BY month ASC