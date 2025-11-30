-- MySQL Test Data for IbexDB Library

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test orders
INSERT INTO orders (customer_id, order_date, total, status) VALUES
    (1, '2024-01-15', 1500.00, 'completed'),
    (1, '2024-02-20', 2500.00, 'completed'),
    (2, '2024-01-10', 500.00, 'completed'),
    (2, '2024-03-05', 750.00, 'completed'),
    (3, '2024-01-25', 3500.00, 'completed'),
    (3, '2024-02-15', 4200.00, 'completed'),
    (4, '2024-03-10', 250.00, 'pending'),
    (5, '2024-01-30', 5500.00, 'completed'),
    (5, '2024-02-28', 6000.00, 'completed'),
    (1, '2024-03-15', 1200.00, 'completed');

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0
);

-- Insert test products
INSERT INTO products (product_name, category, price, stock) VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('Desk Chair', 'Furniture', 199.99, 30),
    ('Standing Desk', 'Furniture', 499.99, 15),
    ('Monitor 27"', 'Electronics', 349.99, 40);

-- Grant permissions
GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'%';
FLUSH PRIVILEGES;

-- Success message
SELECT 'MySQL test data initialized' AS status;

