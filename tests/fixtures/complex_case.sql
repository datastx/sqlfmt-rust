SELECT
    employee_id,
    first_name,
    last_name,
    department,
    salary,
    CASE
        WHEN salary >= 150000 THEN 'executive'
        WHEN salary >= 100000 THEN 'senior'
        WHEN salary >= 70000 THEN 'mid'
        WHEN salary >= 40000 THEN 'junior'
        ELSE 'entry'
    END AS salary_band,
    CASE
        WHEN years_experience >= 10 AND salary >= 100000 THEN 'top performer'
        WHEN years_experience >= 5 OR salary >= 80000 THEN 'experienced'
        ELSE 'developing'
    END AS performance_tier
FROM employees
WHERE department IN ('Engineering', 'Product', 'Design')
    AND status = 'active'
    AND hire_date BETWEEN '2020-01-01' AND '2024-12-31'
ORDER BY department, salary DESC
