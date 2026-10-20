{#
  Silver: cac chuyen hop le (khong co ly do loi). Moi lan chay ghi de dung phan vung thang do.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['source_month'],
        file_format='iceberg'
    )
}}

select
    trip_hash as trip_id,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    source_month,
    _source_file,
    _ingested_at
from {{ ref('flagged_yellow_trips') }}
where size(reject_reasons) = 0