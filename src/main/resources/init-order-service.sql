-- Order Service Database Schema
-- This script creates the schema and tables for the order service
-- It reflects the final state after all Liquibase migrations

CREATE SCHEMA IF NOT EXISTS order_service;

-- Match History Table
-- Stores all completed trades/matches between buyers and sellers
CREATE TABLE IF NOT EXISTS order_service.match_history (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL UNIQUE,   -- Unique match ID from MatchEngine (for idempotency)
    price INTEGER NOT NULL,              -- Deal price
    amount INTEGER NOT NULL,             -- Matched amount
    buyer_uuid UUID NOT NULL,            -- Buyer's user ID
    seller_uuid UUID NOT NULL,           -- Seller's user ID
    order_type VARCHAR(10) NOT NULL,     -- 'BUY' or 'SELL'
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index on match_id for faster idempotency checks
CREATE INDEX IF NOT EXISTS idx_match_history_match_id ON order_service.match_history(match_id);

-- Create indexes for user queries
CREATE INDEX IF NOT EXISTS idx_match_history_buyer ON order_service.match_history(buyer_uuid);
CREATE INDEX IF NOT EXISTS idx_match_history_seller ON order_service.match_history(seller_uuid);

-- Note: The 'orders' table was removed in Liquibase changeSet 6
-- Order data is now managed by the MatchEngine in Redis
