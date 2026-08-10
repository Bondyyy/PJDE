DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS repositories;

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS repositories (
    repo_id BIGINT,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255)
);