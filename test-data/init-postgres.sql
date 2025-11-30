-- PostgreSQL Test Data for IbexDB Library

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    status VARCHAR(20) DEFAULT 'active',
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO users (name, email, age, status, country) VALUES
    ('Alice Smith', 'alice@example.com', 30, 'active', 'USA'),
    ('Bob Johnson', 'bob@example.com', 25, 'active', 'UK'),
    ('Charlie Brown', 'charlie@example.com', 35, 'active', 'Canada'),
    ('Diana Prince', 'diana@example.com', 28, 'active', 'USA'),
    ('Eve Wilson', 'eve@example.com', 32, 'inactive', 'UK'),
    ('Frank Miller', 'frank@example.com', 45, 'active', 'Canada'),
    ('Grace Lee', 'grace@example.com', 27, 'active', 'USA'),
    ('Henry Ford', 'henry@example.com', 40, 'active', 'UK'),
    ('Iris West', 'iris@example.com', 29, 'active', 'Canada'),
    ('Jack Ryan', 'jack@example.com', 38, 'inactive', 'USA');

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    segment VARCHAR(50),
    signup_date DATE DEFAULT CURRENT_DATE
);

-- Insert customer data
INSERT INTO customers (customer_name, email, segment) VALUES
    ('TechCorp Inc', 'contact@techcorp.com', 'Enterprise'),
    ('StartupXYZ', 'hello@startupxyz.com', 'SMB'),
    ('Global Services', 'info@globalservices.com', 'Enterprise'),
    ('Local Shop', 'owner@localshop.com', 'SMB'),
    ('MegaCorp', 'contact@megacorp.com', 'Enterprise');

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO testuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO testuser;

-- Success message
SELECT 'PostgreSQL test data initialized' AS status;

