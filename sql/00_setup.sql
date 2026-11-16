-- Run with an identity that has permission to create catalogs/schemas.
CREATE CATALOG IF NOT EXISTS enterprise_dev;
CREATE SCHEMA IF NOT EXISTS enterprise_dev.lakehouse;

-- For a real workspace, configure an Azure storage credential and external location
-- using your organization's approved identity and governance process.
-- Never place account keys, client secrets, or passwords in this repository.
