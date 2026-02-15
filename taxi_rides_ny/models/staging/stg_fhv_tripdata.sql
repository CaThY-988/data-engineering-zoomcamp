with tripdata as (
  select *
  from {{ source('nytaxi','fhv_tripdata') }}
  where dispatching_base_num is not null
),

renamed as (
  select
      -- identifiers
      dispatching_base_num as dispatching_base_number,
      affiliated_base_number,
      cast(pulocationid as integer) as pickup_location_id,
      cast(dolocationid as integer) as dropoff_location_id,
      
      -- timestamps
      cast(pickup_datetime as timestamp) as pickup_datetime,
      cast(dropoff_datetime as timestamp) as dropoff_datetime,
      
      -- trip info
      cast(sr_flag as integer) as shared_ride_flag

  from tripdata
)

select * from renamed