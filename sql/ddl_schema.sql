-- Target schemas for the Golden Vendor Master (from section 5 of the task document)
-- Run automatically by consolidate_vendor_master.py (IF NOT EXISTS makes it safe to re-run)

-- Table 1: Primary Golden Vendor Master Catalog
CREATE TABLE IF NOT EXISTS gold_vendor_master (
    vendor_id TEXT PRIMARY KEY,
    vendor_name TEXT NOT NULL,
    country TEXT NOT NULL,
    onboarded_date TEXT NOT NULL,
    lead_time_days INTEGER NOT NULL,
    is_lead_time_imputed INTEGER NOT NULL,
    contract_value_usd REAL NOT NULL,
    source_system TEXT NOT NULL,
    last_modified TEXT NOT NULL,
    processed_at TEXT NOT NULL
);

-- Table 2: Normalized Material Categories Bridge Table (1:N)
CREATE TABLE IF NOT EXISTS gold_vendor_categories (
    vendor_id TEXT NOT NULL,
    material_category TEXT NOT NULL,
    PRIMARY KEY (vendor_id, material_category),
    FOREIGN KEY (vendor_id) REFERENCES gold_vendor_master(vendor_id)
);

-- Table 3: Exception Audit Quarantine Vault
CREATE TABLE IF NOT EXISTS err_vendor_quarantine (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id TEXT,
    vendor_name TEXT,
    country TEXT,
    onboarded_date TEXT,
    lead_time_days REAL,
    contract_value_usd TEXT,
    material_categories TEXT,
    source_system TEXT,
    last_modified TEXT,
    rejection_reason TEXT NOT NULL,
    processed_at TEXT NOT NULL
);
