-- db setup
CREATE TABLE IF NOT EXISTS clients (
    user_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    status VARCHAR(20),
    batch INT,
    credit_limit INT,
    interest_rate INT,
    denied_reason VARCHAR(100),
    denied_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loans (
    user_id INT,
    loan_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    due_at TIMESTAMP,
    paid_at TIMESTAMP,
    status VARCHAR(20),
    loan_amount FLOAT,
    tax FLOAT,
    due_amount FLOAT,
    amount_paid FLOAT
);