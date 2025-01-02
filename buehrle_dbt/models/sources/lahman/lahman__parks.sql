with source as (

    select * from {{ source('lahman', 'parks') }}

),

renamed as (

    select
        parkkey as park_id
      , parkname as name
      , parkalias as alias
      , city
      , state
      , country

    from source

)

select * from renamed