-- Verification queries (section 6 of the task document)

-- Query 1: Golden Master Integrity & Total Procurement Spend
SELECT
    vendor_id,
    vendor_name,
    country,
    onboarded_date,
    lead_time_days,
    is_lead_time_imputed,
    contract_value_usd,
    source_system,
    last_modified
FROM gold_vendor_master
ORDER BY vendor_id;

-- Total Portfolio Spend (expected 623650.50)
SELECT ROUND(SUM(contract_value_usd), 2) AS total_portfolio_spend_usd
FROM gold_vendor_master;

-- Query 2: Category Breakdown via Relational Bridge Join
SELECT
    c.material_category,
    COUNT(DISTINCT v.vendor_id) AS supplier_count,
    SUM(v.contract_value_usd) AS category_total_spend
FROM gold_vendor_categories c
JOIN gold_vendor_master v ON c.vendor_id = v.vendor_id
GROUP BY c.material_category
ORDER BY supplier_count DESC, category_total_spend DESC;

-- Query 3: Quarantine Audit Log Review
SELECT
    vendor_id,
    vendor_name,
    source_system,
    rejection_reason
FROM err_vendor_quarantine
ORDER BY vendor_id;
