{{ config(materialized='table') }}

SELECT
    *
FROM semantic_view({{ ref('my_semantic_view') }} metrics revenue, order_count dimensions region, product_category)
)))))__SQLFMT_OUTPUT__(((((
{{ config(materialized="table") }}

select *
from
    semantic_view(
        {{ ref("my_semantic_view") }} metrics revenue
        , order_count dimensions region
        , product_category
    )
