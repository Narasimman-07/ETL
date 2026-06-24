-- Create database
CREATE DATABASE IF NOT EXISTS agridata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE agridata;

-- Districts Table
CREATE TABLE IF NOT EXISTS districts (
    district_code INT PRIMARY KEY,
    district_name VARCHAR(100) NOT NULL,
    UNIQUE KEY idx_district_name (district_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Branches Table
CREATE TABLE IF NOT EXISTS branches (
    branch_id INT PRIMARY KEY,
    branch_name VARCHAR(150) NOT NULL,
    district_code INT NOT NULL,
    FOREIGN KEY (district_code) REFERENCES districts(district_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Market Prices Table
CREATE TABLE IF NOT EXISTS market_prices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    district_code INT NOT NULL,
    branch_id INT NOT NULL,
    price_date DATE NOT NULL,
    item_name VARCHAR(150) NOT NULL,
    min_price DECIMAL(10, 2) NULL,
    max_price DECIMAL(10, 2) NULL,
    qty DECIMAL(10, 2) NULL,
    UNIQUE KEY idx_unique_record (branch_id, price_date, item_name),
    FOREIGN KEY (district_code) REFERENCES districts(district_code),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
