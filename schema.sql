CREATE TABLE IF NOT EXISTS google_ads_spend_daily (
  day TEXT NOT NULL,
  project TEXT,
  customer_id TEXT,
  campaign_id TEXT,
  campaign TEXT,
  ad_group_id TEXT,
  ad_group_name TEXT,
  attr_id TEXT,
  cost REAL DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  impressions INTEGER DEFAULT 0,
  conversions REAL DEFAULT 0,
  daily_budget REAL DEFAULT 0,
  campaign_status TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gam_revenue_daily (
  day TEXT NOT NULL,
  project TEXT,
  network_code TEXT,
  attr_id TEXT,
  ad_unit TEXT,
  revenue REAL DEFAULT 0,
  impressions INTEGER DEFAULT 0,
  clicks INTEGER DEFAULT 0,
  pmr REAL DEFAULT 0,
  ecpm REAL DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  agent TEXT NOT NULL,
  severity TEXT NOT NULL,
  project TEXT,
  attr_id TEXT,
  title TEXT,
  message TEXT,
  status TEXT DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS action_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  project TEXT,
  attr_id TEXT,
  action_type TEXT,
  old_value TEXT,
  new_value TEXT,
  status TEXT,
  message TEXT
);
