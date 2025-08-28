select month, movie_id, tickets_sold, revenue, location
from {{ ref("stg_nj_001") }}
union all
select month, movie_id, tickets_sold, revenue, location
from {{ ref("stg_nj_002") }}
union all
select month, movie_id, tickets_sold, revenue, location
from {{ ref("stg_nj_003") }}
