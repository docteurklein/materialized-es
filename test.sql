drop source if exists event_file cascade;
create source event_file
from file '/home/florian/work/docteurklein/materialized-es/events.txt'
with (tail = true)
format text;

create materialized view event as select
    text::jsonb as payload
from event_file;

create view product_added as select
    (payload->'product_id')::bigint as product_id,
    (payload->>'name') as name,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_added');

create view product_renamed as select
    (payload->'product_id')::bigint as product_id,
    (payload->>'name') as name,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_renamed');

create view category_added as select
    (payload->'category_id')::bigint as category_id,
    (payload->'name') as name,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('category_added');

create view product_in_category as select
    (payload->'product_id')::bigint as product_id,
    (payload->'category_id')::bigint as category_id,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_in_category');

create view product_removed_from_category as select
    (payload->'product_id')::bigint as product_id,
    (payload->'category_id')::bigint as category_id,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_removed_from_category');

create view product_disabled as select
    (payload->'product_id')::bigint as product_id,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_disabled');

create view product_enabled as select
    (payload->'product_id')::bigint as product_id,
    (payload->'at')::bigint as at
from event
where (payload->>'type') in ('product_enabled');

create materialized view active_product as select
    pa.product_id,
    jsonb_agg(coalesce(pr.name, pa.name))->>-1 as name,
    jsonb_agg(distinct c.category_id) as categories
from product_added pa
left join product_renamed pr on (pa.product_id = pr.product_id and pr.at > pa.at)
left join (
    select pic.category_id, pic.product_id
    from product_in_category pic
    where not exists(
        select true from product_removed_from_category prfc
        where prfc.at > pic.at
    )
) c on c.product_id = pa.product_id
where not exists(
    select true from product_disabled pd
    where product_id = pa.product_id
    and exists(
        select true from product_enabled
        where product_id = pa.product_id
        and at > pd.at
    )
)
group by pa.product_id;
