CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT NOT NULL,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS repositories (
    repo_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    url VARCHAR(255),
    PRIMARY KEY (repo_id)
);