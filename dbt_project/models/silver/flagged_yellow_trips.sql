{#
  Buoc chung cho silver va quarantine: doc bronze cua thang can xu ly,
  tao trip_hash, danh dau trung lap va gan ly do loi vao reject_reasons.
  Dong hop le co reject_reasons rong. Ephemeral = khong tao bang, duoc nhung vao 2 model con.
#}
{{ config(materialized='ephemeral') }}

with src as (

    select *
    from {{ source('bronze', 'yellow_trips') }}
    where source_month = '{{ var("batch_month") }}'

),

hashed as (

    select
        *,
        sha2(concat_ws('|',
            coalesce(cast(vendorid as string), ''),
            coalesce(cast(tpep_pickup_datetime as string), ''),
            coalesce(cast(tpep_dropoff_datetime as string), ''),
            coalesce(cast(pulocationid as string), ''),
            coalesce(cast(dolocationid as string), ''),
            coalesce(cast(trip_distance as string), ''),
            coalesce(cast(fare_amount as string), ''),
            coalesce(cast(total_amount as string), '')
        ), 256) as trip_hash
    from src

),

ranked as (

    select
        *,
        row_number() over (
            partition by trip_hash
            order by tpep_pickup_datetime
        ) as dup_rank
    from hashed

)

select
    *,
    array_compact(array(
        case when tpep_pickup_datetime is null or tpep_dropoff_datetime is null
             then 'missing_pickup_or_dropoff' end,
        case when tpep_dropoff_datetime <= tpep_pickup_datetime
             then 'dropoff_before_pickup' end,
        case when unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime) > 86400
             then 'trip_too_long' end,
        case when date_format(tpep_pickup_datetime, 'yyyy-MM') <> source_month
             then 'pickup_outside_month' end,
        case when fare_amount < 0 or total_amount < 0
             then 'negative_amount' end,
        case when trip_distance < 0 or trip_distance > 100
             then 'invalid_distance' end,
        case when passenger_count < 0 or passenger_count > 6
             then 'invalid_passenger_count' end,
        case when dup_rank > 1
             then 'duplicate' end
    )) as reject_reasons
from ranked