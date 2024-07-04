with peoples as (
select * from {{ ref('src_peoples') }}
)

select * from peoples