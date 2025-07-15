CREATE TABLE IF NOT EXISTS dba.tinboxconfig (
    config_id SERIAL PRIMARY KEY,
    config_name VARCHAR(100) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT TRUE,
    subject_filter VARCHAR(255),
    sender_filter VARCHAR(255),
    has_attachment_filter BOOLEAN,
    processed_label VARCHAR(100) DEFAULT 'processed',
    error_label VARCHAR(100) DEFAULT 'errorprocessed',
    download_location VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dba.tinboxconfig IS 'Configuration for checking a Gmail inbox, filtering emails, and processing them.';
COMMENT ON COLUMN dba.tinboxconfig.config_id IS 'Unique identifier for the inbox configuration.';
COMMENT ON COLUMN dba.tinboxconfig.config_name IS 'Descriptive name for the configuration.';
COMMENT ON COLUMN dba.tinboxconfig.is_active IS 'Flag to enable or disable this configuration.';
COMMENT ON COLUMN dba.tinboxconfig.subject_filter IS 'Filter emails by subject line (supports wildcards).';
COMMENT ON COLUMN dba.tinboxconfig.sender_filter IS 'Filter emails by sender address.';
COMMENT ON COLUMN dba.tinboxconfig.has_attachment_filter IS 'Filter emails based on the presence of attachments.';
COMMENT ON COLUMN dba.tinboxconfig.processed_label IS 'The label to apply to successfully processed emails.';
COMMENT ON COLUMN dba.tinboxconfig.error_label IS 'The label to apply to emails that failed processing.';
COMMENT ON COLUMN dba.tinboxconfig.download_location IS 'The local directory to save downloaded emails and attachments.';
COMMENT ON COLUMN dba.tinboxconfig.created_at IS 'Timestamp of when the configuration was created.';
COMMENT ON COLUMN dba.tinboxconfig.last_modified_at IS 'Timestamp of when the configuration was last modified.';

-- Insert a sample configuration for testing
INSERT INTO dba.tinboxconfig (config_name, is_active, subject_filter, sender_filter, has_attachment_filter, download_location)
VALUES ('SampleInvoiceProcessing', TRUE, 'Invoice #', 'billing@example.com', TRUE, '/home/yostfundsadmin/client_etl_workflow/downloads/invoices')
ON CONFLICT (config_name) DO NOTHING;
