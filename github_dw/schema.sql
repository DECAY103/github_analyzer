DROP VIEW IF EXISTS inactive_repos CASCADE;
DROP VIEW IF EXISTS cross_repo_activity CASCADE;
DROP VIEW IF EXISTS rising_contributors CASCADE;
DROP VIEW IF EXISTS monthly_commit_trend CASCADE;

DROP TRIGGER IF EXISTS trg_sync_summary ON Commits CASCADE;
DROP FUNCTION IF EXISTS sync_contributor_summary CASCADE;
DROP FUNCTION IF EXISTS get_top_contributors CASCADE;
DROP FUNCTION IF EXISTS get_language_breakdown CASCADE;
DROP FUNCTION IF EXISTS get_inactive_repos CASCADE;

DROP TABLE IF EXISTS Contributor_Summary CASCADE;
DROP TABLE IF EXISTS Commits CASCADE;
DROP TABLE IF EXISTS Repository_Languages CASCADE;
DROP TABLE IF EXISTS Languages CASCADE;
DROP TABLE IF EXISTS Repositories CASCADE;
DROP TABLE IF EXISTS Users CASCADE;

CREATE TABLE Users (
    user_id         INT PRIMARY KEY,
    github_username VARCHAR(255) UNIQUE NOT NULL,
    name            VARCHAR(255),
    email           VARCHAR(255)
);

CREATE TABLE Repositories (
    repo_id         INT PRIMARY KEY,
    repo_name       VARCHAR(255) NOT NULL,
    owner_id        INT NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
    description     TEXT,
    last_synced_at  TIMESTAMP
);

CREATE TABLE Languages (
    language_id     SERIAL PRIMARY KEY,
    language_name   VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Repository_Languages (
    repo_id         INT REFERENCES Repositories(repo_id) ON DELETE CASCADE,
    language_id     INT REFERENCES Languages(language_id) ON DELETE CASCADE,
    bytes_written   INT,
    PRIMARY KEY (repo_id, language_id)
);

CREATE TABLE Commits (
    commit_sha          VARCHAR(40) PRIMARY KEY,
    repo_id             INT NOT NULL REFERENCES Repositories(repo_id) ON DELETE CASCADE,
    author_id           INT NOT NULL REFERENCES Users(user_id),
    message             TEXT,
    commit_timestamp    TIMESTAMP NOT NULL,
    lines_added         INT DEFAULT 0,
    lines_deleted       INT DEFAULT 0
);

CREATE TABLE Contributor_Summary (
    user_id             INT REFERENCES Users(user_id) ON DELETE CASCADE,
    repo_id             INT REFERENCES Repositories(repo_id) ON DELETE CASCADE,
    total_commits       INT DEFAULT 0,
    latest_commit_date  TIMESTAMP,
    PRIMARY KEY (user_id, repo_id)
);

CREATE INDEX idx_commits_repo_id ON Commits(repo_id);
CREATE INDEX idx_commits_author_id ON Commits(author_id);
CREATE INDEX idx_commits_timestamp ON Commits(commit_timestamp);
CREATE INDEX idx_commits_author_repo ON Commits(author_id, repo_id);
CREATE INDEX idx_summary_repo ON Contributor_Summary(repo_id);
CREATE INDEX idx_summary_total ON Contributor_Summary(total_commits DESC);

CREATE OR REPLACE FUNCTION sync_contributor_summary()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO Contributor_Summary (user_id, repo_id, total_commits, latest_commit_date)
    VALUES (NEW.author_id, NEW.repo_id, 1, NEW.commit_timestamp)
    ON CONFLICT (user_id, repo_id) DO UPDATE
        SET total_commits      = Contributor_Summary.total_commits + 1,
            latest_commit_date = GREATEST(Contributor_Summary.latest_commit_date, EXCLUDED.latest_commit_date);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_summary
AFTER INSERT ON Commits
FOR EACH ROW
EXECUTE FUNCTION sync_contributor_summary();

CREATE VIEW monthly_commit_trend AS
WITH monthly AS (
    SELECT r.repo_name,
           DATE_TRUNC('month', c.commit_timestamp) AS month,
           COUNT(*) AS commit_count
    FROM Commits c
    JOIN Repositories r ON r.repo_id = c.repo_id
    GROUP BY r.repo_name, month
)
SELECT *,
       LAG(commit_count) OVER (PARTITION BY repo_name ORDER BY month) AS prev_month,
       commit_count - LAG(commit_count) OVER (PARTITION BY repo_name ORDER BY month) AS delta
FROM monthly ORDER BY repo_name, month;

CREATE VIEW rising_contributors AS
SELECT u.github_username,
       cs.total_commits,
       cs.latest_commit_date,
       MIN(c.commit_timestamp) AS first_seen,
       EXTRACT(DAY FROM NOW() - MIN(c.commit_timestamp))::INT AS days_active
FROM Contributor_Summary cs
JOIN Users u ON u.user_id = cs.user_id
JOIN Commits c ON c.author_id = cs.user_id
GROUP BY u.github_username, cs.total_commits, cs.latest_commit_date
HAVING MIN(c.commit_timestamp) > NOW() - INTERVAL '60 days'
   AND cs.total_commits >= 5
ORDER BY cs.total_commits DESC;

CREATE VIEW cross_repo_activity AS
SELECT u.github_username,
       r.repo_name,
       cs.total_commits,
       cs.latest_commit_date,
       RANK() OVER (PARTITION BY r.repo_id ORDER BY cs.total_commits DESC) AS rank_in_repo
FROM Contributor_Summary cs
JOIN Users u ON u.user_id = cs.user_id
JOIN Repositories r ON r.repo_id = cs.repo_id;

CREATE VIEW inactive_repos AS
SELECT r.repo_name,
       u.github_username AS owner,
       MAX(c.commit_timestamp) AS last_commit,
       EXTRACT(DAY FROM NOW() - MAX(c.commit_timestamp))::INT AS days_silent
FROM Repositories r
JOIN Users u ON u.user_id = r.owner_id
LEFT JOIN Commits c ON c.repo_id = r.repo_id
GROUP BY r.repo_id, r.repo_name, u.github_username
HAVING MAX(c.commit_timestamp) < NOW() - INTERVAL '30 days'
    OR MAX(c.commit_timestamp) IS NULL;

CREATE OR REPLACE FUNCTION get_top_contributors(limit_n INT DEFAULT 10)
RETURNS TABLE (username VARCHAR, total_commits BIGINT, repos_active INT) AS $$
BEGIN
    RETURN QUERY
    SELECT u.github_username,
           SUM(cs.total_commits)::BIGINT,
           COUNT(DISTINCT cs.repo_id)::INT
    FROM Contributor_Summary cs
    JOIN Users u ON u.user_id = cs.user_id
    GROUP BY u.github_username
    ORDER BY SUM(cs.total_commits) DESC
    LIMIT limit_n;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_language_breakdown()
RETURNS TABLE (language VARCHAR, total_bytes BIGINT, repo_count BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT l.language_name,
           SUM(rl.bytes_written)::BIGINT,
           COUNT(DISTINCT rl.repo_id)::BIGINT
    FROM Repository_Languages rl
    JOIN Languages l ON l.language_id = rl.language_id
    GROUP BY l.language_name
    ORDER BY total_bytes DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_inactive_repos(days_threshold INT DEFAULT 30)
RETURNS TABLE (repo_name VARCHAR, owner VARCHAR, last_commit TIMESTAMP, days_silent INT) AS $$
BEGIN
    RETURN QUERY
    SELECT r.repo_name,
           u.github_username,
           MAX(c.commit_timestamp),
           EXTRACT(DAY FROM NOW() - MAX(c.commit_timestamp))::INT
    FROM Repositories r
    JOIN Users u ON u.user_id = r.owner_id
    LEFT JOIN Commits c ON c.repo_id = r.repo_id
    GROUP BY r.repo_id, r.repo_name, u.github_username
    HAVING MAX(c.commit_timestamp) < NOW() - (days_threshold || ' days')::INTERVAL
        OR MAX(c.commit_timestamp) IS NULL
    ORDER BY last_commit ASC NULLS FIRST;
END;
$$ LANGUAGE plpgsql;
