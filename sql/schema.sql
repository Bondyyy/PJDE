CREATE TABLE actor (
    actor_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
);

CREATE TABLE repositories (
    repo_id BIGINT,
    name VARCHAR(255),
    url VARCHAR(255),
);