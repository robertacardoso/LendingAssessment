-- db setup
DROP TABLE IF EXISTS clients;
DROP TABLE IF EXISTS loans;

CREATE TABLE clients (
    user_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    batch INT,
    credit_limit INT,
    interest_rate INT,
    denied_reason VARCHAR(100),
    denied_at TIMESTAMP
);

CREATE TABLE loans (
    user_id INT NOT NULL,
    loan_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    due_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    loan_amount FLOAT,
    tax FLOAT,
    due_amount FLOAT,
    amount_paid FLOAT
);
