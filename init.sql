-- Initialize the database with proper settings
-- This file is executed when the PostgreSQL container starts for the first time

-- Set timezone
SET timezone = 'UTC';

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Set up proper encoding
ALTER DATABASE ifrs9pro_db SET default_text_search_config = 'english';

-- Create any additional users or permissions if needed
-- (The main user is created by the POSTGRES_USER environment variable)
