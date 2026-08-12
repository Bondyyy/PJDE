USE github_data;


CREATE TABLE IF NOT EXISTS users_log_before (
    log_id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (log_id)
);


CREATE TABLE IF NOT EXISTS users_log_after (
    log_id BIGINT NOT NULL AUTO_INCREMENT,
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (log_id)
);


DROP TRIGGER IF EXISTS before_insert_user_log;
DROP TRIGGER IF EXISTS before_update_user_log;
DROP TRIGGER IF EXISTS before_delete_user_log;
DROP TRIGGER IF EXISTS after_insert_user_log;
DROP TRIGGER IF EXISTS after_update_user_log;
DROP TRIGGER IF EXISTS after_delete_user_log;


DELIMITER //


CREATE TRIGGER before_insert_user_log
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before (user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.url, NEW.avatar_url, 'INSERT');
END//


CREATE TRIGGER before_update_user_log
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before (user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'UPDATE');
END//


CREATE TRIGGER before_delete_user_log
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before (user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'DELETE');
END//

CREATE TRIGGER after_insert_user_log
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after (user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (
        NEW.user_id, NEW.login, NEW.gravatar_id, NEW.url, NEW.avatar_url, 'INSERT'
    );
END//


CREATE TRIGGER after_update_user_log
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after ( user_id, login,
        gravatar_id, url, avatar_url, state
    )
    VALUES (
        NEW.user_id, NEW.login, NEW.gravatar_id,
        NEW.url, NEW.avatar_url, 'UPDATE'
    );
END//


CREATE TRIGGER after_delete_user_log
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after (user_id, login, gravatar_id,
        url, avatar_url, state
    )
    VALUES ( OLD.user_id, OLD.login, OLD.gravatar_id,
        OLD.url, OLD.avatar_url, 'DELETE'
    );
END//


DELIMITER ;