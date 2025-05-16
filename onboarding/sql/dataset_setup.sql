-- Description: Sets up tables in the public schema to track dataset metadata for the ETL pipeline.
-- Naming Conventions: t for tables, f for functions, p for procedures, v for views.
-- Idempotency: Each object creation is guarded by an existence check to prevent errors on rerun.

-- Line 1: Create tDataSetType table if it doesn't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 1: Starting creation of tDataSetType table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tdatasettype') THEN
        CREATE TABLE public.tDataSetType (
            DataSetTypeID SERIAL PRIMARY KEY,
            TypeName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE public.tDataSetType IS 'Stores dataset type definitions (e.g., MeetMax, ClientUpload).';
        COMMENT ON COLUMN public.tDataSetType.DataSetTypeID IS 'Primary key for dataset type.';
        COMMENT ON COLUMN public.tDataSetType.TypeName IS 'Unique name of the dataset type.';
        COMMENT ON COLUMN public.tDataSetType.Description IS 'Optional description of the dataset type.';
        COMMENT ON COLUMN public.tDataSetType.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN public.tDataSetType.CreatedBy IS 'User who created the record.';
        RAISE NOTICE 'Line 15: tDataSetType table and comments created';
    END IF;
    RAISE NOTICE 'Line 17: Completed tDataSetType block';
END $$;

-- Line 20: Create tDataSource table if it doesn't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 20: Starting creation of tDataSource table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tdatasource') THEN
        CREATE TABLE public.tDataSource (
            DataSourceID SERIAL PRIMARY KEY,
            SourceName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE public.tDataSource IS 'Stores data source definitions (e.g., MeetMax Website, SFTP Upload).';
        COMMENT ON COLUMN public.tDataSource.DataSourceID IS 'Primary key for data source.';
        COMMENT ON COLUMN public.tDataSource.SourceName IS 'Unique name of the data source.';
        COMMENT ON COLUMN public.tDataSource.Description IS 'Optional description of the data source.';
        COMMENT ON COLUMN public.tDataSource.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN public.tDataSource.CreatedBy IS 'User who created the record.';
        RAISE NOTICE 'Line 34: tDataSource table and comments created';
    END IF;
    RAISE NOTICE 'Line 36: Completed tDataSource block';
END $$;

-- Line 39: Create tDataStatus table if it doesn't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 39: Starting creation of tDataStatus table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tdatastatus') THEN
        CREATE TABLE public.tDataStatus (
            DataStatusID SERIAL PRIMARY KEY,
            StatusName VARCHAR(50) NOT NULL UNIQUE,
            Description TEXT,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER
        );

        COMMENT ON TABLE public.tDataStatus IS 'Stores status codes for datasets (e.g., Active, Inactive, Deleted).';
        COMMENT ON COLUMN public.tDataStatus.DataStatusID IS 'Primary key for status.';
        COMMENT ON COLUMN public.tDataStatus.StatusName IS 'Unique name of the status.';
        COMMENT ON COLUMN public.tDataStatus.Description IS 'Optional description of the status.';
        COMMENT ON COLUMN public.tDataStatus.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN public.tDataStatus.CreatedBy IS 'User who created the record.';
        RAISE NOTICE 'Line 53: tDataStatus table and comments created';
    END IF;
    RAISE NOTICE 'Line 55: Completed tDataStatus block';
END $$;

-- Line 58: Create tDataSet table if it doesn't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 58: Starting creation of tDataSet table';
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tdataset') THEN
        CREATE TABLE public.tDataSet (
            DataSetID SERIAL PRIMARY KEY,
            DataSetDate DATE NOT NULL,
            Label VARCHAR(100) NOT NULL,
            DataSetTypeID INTEGER NOT NULL,
            DataSourceID INTEGER NOT NULL,
            DataStatusID INTEGER NOT NULL,
            EffFromDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            EffThruDate TIMESTAMP NOT NULL DEFAULT '9999-01-01',
            IsActive BOOLEAN NOT NULL DEFAULT TRUE,
            CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CreatedBy VARCHAR(50) NOT NULL DEFAULT CURRENT_USER,
            CONSTRAINT fk_dataset_type FOREIGN KEY (DataSetTypeID) REFERENCES public.tDataSetType (DataSetTypeID),
            CONSTRAINT fk_dataset_source FOREIGN KEY (DataSourceID) REFERENCES public.tDataSource (DataSourceID),
            CONSTRAINT fk_dataset_status FOREIGN KEY (DataStatusID) REFERENCES public.tDataStatus (DataStatusID),
            CONSTRAINT chk_eff_dates CHECK (EffFromDate <= EffThruDate)
        );

        COMMENT ON TABLE public.tDataSet IS 'Tracks metadata for dataset loads in the ETL pipeline.';
        COMMENT ON COLUMN public.tDataSet.DataSetID IS 'Primary key for the dataset.';
        COMMENT ON COLUMN public.tDataSet.DataSetDate IS 'Date associated with the dataset (e.g., data reference date).';
        COMMENT ON COLUMN public.tDataSet.Label IS 'Descriptive label for the dataset.';
        COMMENT ON COLUMN public.tDataSet.DataSetTypeID IS 'Foreign key to tDataSetType, indicating dataset type.';
        COMMENT ON COLUMN public.tDataSet.DataSourceID IS 'Foreign key to tDataSource, indicating data source.';
        COMMENT ON COLUMN public.tDataSet.DataStatusID IS 'Foreign key to tDataStatus, indicating dataset status.';
        COMMENT ON COLUMN public.tDataSet.EffFromDate IS 'Effective start date, defaults to creation time.';
        COMMENT ON COLUMN public.tDataSet.EffThruDate IS 'Effective end date, defaults to 9999-01-01 for active records.';
        COMMENT ON COLUMN public.tDataSet.IsActive IS 'Indicates if the dataset is active (TRUE) or inactive (FALSE).';
        COMMENT ON COLUMN public.tDataSet.CreatedDate IS 'Timestamp when the record was created.';
        COMMENT ON COLUMN public.tDataSet.CreatedBy IS 'User who created the record.';
        RAISE NOTICE 'Line 85: tDataSet table and comments created';
    END IF;
    RAISE NOTICE 'Line 87: Completed tDataSet block';
END $$;

-- Line 90: Create indexes for tDataSet if they don't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 90: Starting creation of tDataSet indexes';
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_tdataset_datasetdate') THEN
        CREATE INDEX idx_tdataset_datasetdate ON public.tDataSet (DataSetDate);
        RAISE NOTICE 'Line 94: idx_tdataset_datasetdate index created';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_tdataset_isactive') THEN
        CREATE INDEX idx_tdataset_isactive ON public.tDataSet (IsActive);
        RAISE NOTICE 'Line 98: idx_tdataset_isactive index created';
    END IF;
    RAISE NOTICE 'Line 100: Completed tDataSet indexes block';
END $$;


--- Line ~103: Create function fEnforceSingleActiveDataSet if it doesn't exist
DO $OUTER$
BEGIN
    RAISE NOTICE 'Line 103: Starting creation of fEnforceSingleActiveDataSet function';
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        AND proname = 'fenforcesingleactivedataset'
    ) THEN
        CREATE FUNCTION public.fEnforceSingleActiveDataSet()
        RETURNS TRIGGER AS $INNER$
        BEGIN
            RAISE NOTICE 'Line 109: Inside fEnforceSingleActiveDataSet function body';
            IF NEW.IsActive = TRUE THEN
                UPDATE public.tDataSet
                SET IsActive = FALSE,
                    EffThruDate = CURRENT_TIMESTAMP,
                    DataStatusID = (SELECT DataStatusID FROM public.tDataStatus WHERE StatusName = 'Inactive')
                WHERE Label = NEW.Label
                  AND DataSetTypeID = NEW.DataSetTypeID
                  AND DataSetDate = NEW.DataSetDate
                  AND DataSetID != NEW.DataSetID
                  AND IsActive = TRUE;
                RAISE NOTICE 'Line 119: Completed UPDATE in fEnforceSingleActiveDataSet';
            END IF;
            RAISE NOTICE 'Line 122: Returning from fEnforceSingleActiveDataSet';
            RETURN NEW;
        END;
        $INNER$ LANGUAGE plpgsql;
        RAISE NOTICE 'Line 125: fEnforceSingleActiveDataSet function created';
    END IF;
    RAISE NOTICE 'Line 127: Completed fEnforceSingleActiveDataSet block';
END $OUTER$;

-- Line 130: Create trigger tTriggerEnforceSingleActiveDataSet if it doesn't exist
DO $$
BEGIN
    RAISE NOTICE 'Line 130: Starting creation of tTriggerEnforceSingleActiveDataSet trigger';
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ttriggerenforcesingleactivedataset' AND tgrelid = (SELECT oid FROM pg_class WHERE relname = 'tdataset' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public'))) THEN
        CREATE TRIGGER tTriggerEnforceSingleActiveDataSet
        AFTER INSERT OR UPDATE OF IsActive
        ON public.tDataSet
        FOR EACH ROW
        WHEN (NEW.IsActive = TRUE)
        EXECUTE FUNCTION public.fEnforceSingleActiveDataSet();
        RAISE NOTICE 'Line 138: tTriggerEnforceSingleActiveDataSet trigger created';
    END IF;
    RAISE NOTICE 'Line 140: Completed tTriggerEnforceSingleActiveDataSet block';
END $$;

-- Line 143: Insert initial data for lookup tables only if they are empty
DO $$
BEGIN
    RAISE NOTICE 'Line 143: Starting insert into tDataSetType';
    IF (SELECT COUNT(*) FROM public.tDataSetType) = 0 THEN
        INSERT INTO public.tDataSetType (TypeName, Description) VALUES
            ('MeetMax', 'Datasets from MeetMax event scraping'),
            ('ClientUpload', 'Datasets uploaded by clients via SFTP');
        RAISE NOTICE 'Line 148: Inserted data into tDataSetType';
    END IF;
    RAISE NOTICE 'Line 150: Completed tDataSetType insert block';
END $$;

DO $$
BEGIN
    RAISE NOTICE 'Line 154: Starting insert into tDataSource';
    IF (SELECT COUNT(*) FROM public.tDataSource) = 0 THEN
        INSERT INTO public.tDataSource (SourceName, Description) VALUES
            ('MeetMaxWebsite', 'Data scraped from MeetMax website'),
            ('SFTPUpload', 'Data uploaded by clients via SFTP');
        RAISE NOTICE 'Line 159: Inserted data into tDataSource';
    END IF;
    RAISE NOTICE 'Line 161: Completed tDataSource insert block';
END $$;

DO $$
BEGIN
    RAISE NOTICE 'Line 165: Starting insert into tDataStatus';
    IF (SELECT COUNT(*) FROM public.tDataStatus) = 0 THEN
        INSERT INTO public.tDataStatus (StatusName, Description) VALUES
            ('Active', 'Dataset is currently active and in use'),
            ('Inactive', 'Dataset is no longer active but retained for history'),
            ('Deleted', 'Dataset has been marked for deletion');
        RAISE NOTICE 'Line 170: Inserted data into tDataStatus';
    END IF;
    RAISE NOTICE 'Line 172: Completed tDataStatus insert block';
END $$;