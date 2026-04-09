CREATE SCHEMA IF NOT EXISTS order_service;

CREATE TABLE IF NOT EXISTS order_service.orders (
    id SERIAL PRIMARY KEY,
    type VARCHAR(10) NOT NULL,          -- 'BUY' or 'SELL'
    price INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    user_uuid UUID NOT NULL,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING_ASSET_CHECK'
);

CREATE TABLE IF NOT EXISTS order_service.match_history (
    id SERIAL PRIMARY KEY,
    price INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    buyer_uuid UUID NOT NULL,
    seller_uuid UUID NOT NULL,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
