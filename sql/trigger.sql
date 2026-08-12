CREATE TABLE IF NOT EXISTS users_log (
    log_id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    state VARCHAR(20),
    log_timestamp TIMESTAMP(3)
        DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (log_id)
);

DROP TRIGGER IF EXISTS after_insert_user_log;
DROP TRIGGER IF EXISTS after_update_user_log;
DROP TRIGGER IF EXISTS after_delete_user_log;

DELIMITER //

CREATE TRIGGER after_insert_user_log
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log (
        user_id,
        login,
        gravatar_id,
        url,
        avatar_url,
        state
    )
    VALUES (
        NEW.user_id,
        NEW.login,
        NEW.gravatar_id,
        NEW.url,
        NEW.avatar_url,
        'INSERT'
    );
END//


CREATE TRIGGER after_update_user_log
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log (
        user_id,
        login,
        gravatar_id,
        url,
        avatar_url,
        state
    )
    VALUES (
        NEW.user_id,
        NEW.login,
        NEW.gravatar_id,
        NEW.url,
        NEW.avatar_url,
        'UPDATE'
    );
END//


CREATE TRIGGER after_delete_user_log
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log (
        user_id,
        login,
        gravatar_id,
        url,
        avatar_url,
        state
    )
    VALUES (
        OLD.user_id,
        OLD.login,
        OLD.gravatar_id,
        OLD.url,
        OLD.avatar_url,
        'DELETE'
    );
END//

DELIMITER ;

CREATE TABLE IF NOT EXISTS repositories_log (
    log_id BIGINT NOT NULL AUTO_INCREMENT,
    repo_id BIGINT,
    name VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(20),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (log_id)
);

DROP TRIGGER IF EXISTS after_insert_repository_log;
DROP TRIGGER IF EXISTS after_update_repository_log;
DROP TRIGGER IF EXISTS after_delete_repository_log;

DELIMITER //

CREATE TRIGGER after_insert_repository_log
AFTER INSERT ON repositories
FOR EACH ROW
BEGIN
    INSERT INTO repositories_log (
        repo_id,
        name,
        url,
        state
    )
    VALUES (
        NEW.repo_id,
        NEW.name,
        NEW.url,
        'INSERT'
    );
END//


CREATE TRIGGER after_update_repository_log
AFTER UPDATE ON repositories
FOR EACH ROW
BEGIN
    INSERT INTO repositories_log (
        repo_id,
        name,
        url,
        state
    )
    VALUES (
        NEW.repo_id,
        NEW.name,
        NEW.url,
        'UPDATE'
    );
END//


CREATE TRIGGER after_delete_repository_log
AFTER DELETE ON repositories
FOR EACH ROW
BEGIN
    INSERT INTO repositories_log (
        repo_id,
        name,
        url,
        state
    )
    VALUES (
        OLD.repo_id,
        OLD.name,
        OLD.url,
        'DELETE'
    );
END//

DELIMITER ;

CREATE TABLE IF NOT EXISTS kafka_checkpoint (
    entity VARCHAR(50) NOT NULL,
    last_log_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(3)
        DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (entity)
);

INSERT IGNORE INTO kafka_checkpoint (
    entity,
    last_log_id
)
VALUES
    ('users', 0),
    ('repositories', 0);