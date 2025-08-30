select
    movie_id,
    movie_title,
    release_date,
    coalesce(genre, 'Unknown') as genre,
    country,
    studio,
    budget,
    director,
    rating,
    minutes
from {{ source("dbt_cinema_analytics", "movie_catalogue") }}
