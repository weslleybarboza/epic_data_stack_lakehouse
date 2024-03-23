WITH date_range AS (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="CAST('2024-01-01' AS DATE)",
        end_date="CAST('2027-01-01' AS DATE)"
    ) }}
),
calendar_with_weeks AS (
    SELECT
        DATE_FORMAT(a.date_day, '%Y%m%d') AS key, -- %d represents day of the month (00..31)
        cast( a.date_day as date) as date_day,
        DATE_FORMAT(a.date_day, '%d') AS day_of_month, -- %d represents day of the month (00..31)
        DATE_FORMAT(a.date_day, '%m') AS month_no, -- %m represents month (01..12)
        DATE_FORMAT(a.date_day, '%Y') AS year_no, -- %Y represents year (4-digit year)
        DATE_FORMAT(DATE_TRUNC('week', a.date_day), '%Y-%m-%d') AS week_start_date, -- Truncate to week start and format as YYYY-MM-DD
        DATE_FORMAT(DATE_TRUNC('week', a.date_day) + INTERVAL '6' DAY, '%Y-%m-%d') AS week_end_date, -- Add 6 days to truncate to week start and format as YYYY-MM-DD
        DATE_FORMAT(a.date_day, '%M') AS month_desc -- %m represents month (01..12)
    FROM date_range a
)
SELECT * FROM calendar_with_weeks
