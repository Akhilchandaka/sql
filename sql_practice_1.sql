/* =========================================================
   BASIC SELECT & FILTER
   ========================================================= */

SELECT * 
FROM dataset.employees;

SELECT employee_id, name, salary
FROM dataset.employees
WHERE salary > 50000;


/* =========================================================
   AGGREGATIONS
   ========================================================= */

SELECT department_id, COUNT(*) AS emp_count
FROM dataset.employees
GROUP BY department_id;

SELECT department_id, AVG(salary) AS avg_salary
FROM dataset.employees
GROUP BY department_id
HAVING AVG(salary) > 60000;


/* =========================================================
   JOINS
   ========================================================= */

SELECT e.employee_id, e.name, d.department_name
FROM dataset.employees e
INNER JOIN dataset.departments d
ON e.department_id = d.department_id;

SELECT e.name, d.department_name
FROM dataset.employees e
LEFT JOIN dataset.departments d
ON e.department_id = d.department_id;


/* =========================================================
   NULL HANDLING
   ========================================================= */

SELECT name, IFNULL(salary, 0) AS salary
FROM dataset.employees;

SELECT name, COALESCE(bonus, commission, 0) AS final_bonus
FROM dataset.employees;


/* =========================================================
   SUBQUERIES
   ========================================================= */

SELECT *
FROM dataset.employees
WHERE salary > (
  SELECT AVG(salary) FROM dataset.employees
);


/* =========================================================
   WINDOW FUNCTIONS
   ========================================================= */

SELECT name, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num,
       RANK() OVER (ORDER BY salary DESC) AS rnk,
       DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rnk
FROM dataset.employees;

SELECT name, salary,
       LAG(salary) OVER (ORDER BY salary) AS prev_salary,
       LEAD(salary) OVER (ORDER BY salary) AS next_salary
FROM dataset.employees;


/* =========================================================
   DUPLICATE REMOVAL
   ========================================================= */

SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY employee_id 
           ORDER BY updated_at DESC
         ) AS rn
  FROM dataset.employees
)
WHERE rn = 1;


/* =========================================================
   CTE (WITH CLAUSE)
   ========================================================= */

WITH dept_avg AS (
  SELECT department_id, AVG(salary) AS avg_salary
  FROM dataset.employees
  GROUP BY department_id
)
SELECT e.*
FROM dataset.employees e
JOIN dept_avg d
ON e.department_id = d.department_id
WHERE e.salary > d.avg_salary;


/* =========================================================
   MERGE (UPSERT)
   ========================================================= */

MERGE dataset.employees T
USING dataset.staging_employees S
ON T.employee_id = S.employee_id
WHEN MATCHED THEN
  UPDATE SET
    name = S.name,
    salary = S.salary
WHEN NOT MATCHED THEN
  INSERT (employee_id, name, salary)
  VALUES (S.employee_id, S.name, S.salary);


/* =========================================================
   DATE FUNCTIONS
   ========================================================= */

SELECT CURRENT_DATE() AS today;

SELECT employee_id
FROM dataset.employees
WHERE DATE(joining_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);


/* =========================================================
   ARRAY & STRUCT (BIGQUERY SPECIFIC)
   ========================================================= */

SELECT name, skill
FROM dataset.employees,
UNNEST(skills) AS skill;


/* =========================================================
   STORED PROCEDURE
   ========================================================= */

CREATE OR REPLACE PROCEDURE dataset.load_employees()
BEGIN
  DECLARE row_count INT64;

  SET row_count = (
    SELECT COUNT(*) FROM dataset.staging_employees
  );

  IF row_count > 0 THEN
    MERGE dataset.employees T
    USING dataset.staging_employees S
    ON T.employee_id = S.employee_id
    WHEN MATCHED THEN
      UPDATE SET
        name = S.name,
        salary = S.salary
    WHEN NOT MATCHED THEN
      INSERT (employee_id, name, salary)
      VALUES (S.employee_id, S.name, S.salary);
  END IF;
END;


/* =========================================================
   CALL PROCEDURE
   ========================================================= */

CALL dataset.load_employees();


/* =====================================================
   ADVANCED BIGQUERY PRACTICE – FEATURE BRANCH
   ===================================================== */

-- Top 3 salaries per department
SELECT *
FROM (
  SELECT *,
         DENSE_RANK() OVER (
           PARTITION BY department_id
           ORDER BY salary DESC
         ) AS dept_rank
  FROM dataset.employees
)
WHERE dept_rank <= 3;

-- Employees hired in last 90 days
SELECT *
FROM dataset.employees
WHERE DATE(joining_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);