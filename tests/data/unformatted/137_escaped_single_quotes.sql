SELECT
    CASE
        WHEN category = 'type_a'
        THEN 'Men''s Basketball'
        WHEN category = 'type_b'
        THEN 'Women''s Basketball'
    END AS activity_name
)))))__SQLFMT_OUTPUT__(((((
select
    case
        when category = 'type_a'
        then 'Men''s Basketball'
        when category = 'type_b'
        then 'Women''s Basketball'
    end as activity_name
