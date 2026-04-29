# Отчёт по пролонгациям (2023)

Расчёт K1 и K2 — насколько менеджеры продлевают завершённые проекты.

## Результаты

Три CSV:

- `department_year_report.csv` — отдел по месяцам
- `manager_year_report.csv` — менеджеры за год
- `manager_monthly_coeffs.csv` — менеджеры по месяцам

## Google Таблица

[https://docs.google.com/spreadsheets/d/10fOf69-iKudPPBgprOcojHlXJ6aLuZaeaGuDdcR-rWk/edit?gid=1614219230#gid=1614219230](google_sheets/report_link.txt)

## Логика

- **K1** — пролонгация в первый месяц после завершения
- **K2** — пролонгация во второй месяц (если в первом не было)
- `K = сумма отгрузки в месяц пролонгации / сумма отгрузки в последний месяц`