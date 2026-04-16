import os
import requests
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_PAT = os.getenv("GITHUB_PAT")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "github_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

REPOS = [
    ("psf", "requests"),
    ("tiangolo", "fastapi"),
    ("pallets", "flask")
]

def get_headers():
    if GITHUB_PAT and GITHUB_PAT != "":
        return {
            "Authorization": f"token {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json"
        }
    return {"Accept": "application/vnd.github.v3+json"}

def check_rate_limit(response):
    if "X-RateLimit-Remaining" in response.headers:
        remaining = int(response.headers["X-RateLimit-Remaining"])
        if remaining < 10:
            reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_time = max(reset_time - int(time.time()), 0) + 1
            print(f"Rate limit low (<10). Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def ingest():
    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return
        
    conn.autocommit = False
    headers = get_headers()
    
    for owner, repo in REPOS:
        print(f"\\n--- Processing {owner}/{repo} ---")
        try:
            with conn.cursor() as cur:
                # GET repo metadata
                repo_url = f"https://api.github.com/repos/{owner}/{repo}"
                repo_resp = requests.get(repo_url, headers=headers)
                if repo_resp.status_code != 200:
                    print(f"Failed to fetch repo {owner}/{repo}: {repo_resp.text}")
                    continue
                check_rate_limit(repo_resp)
                repo_data = repo_resp.json()
                
                # Upsert owner into Users
                owner_data = repo_data.get("owner", {})
                if not owner_data:
                    print(f"No owner data for {owner}/{repo}. Skipping.")
                    continue
                    
                owner_id = owner_data["id"]
                owner_login = owner_data["login"]
                
                cur.execute("""
                    INSERT INTO Users (user_id, github_username, name, email)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET github_username = EXCLUDED.github_username
                """, (owner_id, owner_login, None, None))
                
                # Upsert Repo
                repo_id = repo_data["id"]
                repo_name = repo_data["name"]
                description = repo_data.get("description", "")
                
                # Get last_synced_at to use as 'since' parameter
                cur.execute("SELECT last_synced_at FROM Repositories WHERE repo_id = %s", (repo_id,))
                row = cur.fetchone()
                last_synced_at = row[0] if row else None
                
                cur.execute("""
                    INSERT INTO Repositories (repo_id, repo_name, owner_id, description, last_synced_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (repo_id) DO UPDATE
                    SET repo_name = EXCLUDED.repo_name,
                        description = EXCLUDED.description
                """, (repo_id, repo_name, owner_id, description, last_synced_at))
                
                # Fetch Commits
                commits_url = f"https://api.github.com/repos/{owner}/{repo}/commits"
                params = {"per_page": 100}
                if last_synced_at:
                    params["since"] = last_synced_at.isoformat()
                    
                page = 1
                commits_ingested = 0
                while True:
                    params["page"] = page
                    c_resp = requests.get(commits_url, headers=headers, params=params)
                    if c_resp.status_code != 200:
                        break
                    check_rate_limit(c_resp)
                    commits_data = c_resp.json()
                    if not commits_data:
                        break
                    
                    for c in commits_data:
                        sha = c["sha"]
                        message = c["commit"]["message"]
                        ts_str = c["commit"]["author"]["date"]
                        commit_timestamp = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
                        
                        author = c.get("author")
                        if author and author.get("id"):
                            author_id = author["id"]
                            author_login = author["login"]
                            
                            # Upsert commit author
                            cur.execute("""
                                INSERT INTO Users (user_id, github_username, name, email)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (user_id) DO UPDATE
                                SET github_username = EXCLUDED.github_username
                            """, (author_id, author_login, c["commit"]["author"].get("name"), c["commit"]["author"].get("email")))
                            
                            # Insert commit
                            cur.execute("""
                                INSERT INTO Commits (commit_sha, repo_id, author_id, message, commit_timestamp, lines_added, lines_deleted)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (commit_sha) DO NOTHING
                            """, (sha, repo_id, author_id, message, commit_timestamp, 0, 0))
                            commits_ingested += 1
                    
                    if len(commits_data) < 100:
                        break
                    page += 1
                
                # Fetch Languages
                lang_url = f"https://api.github.com/repos/{owner}/{repo}/languages"
                l_resp = requests.get(lang_url, headers=headers)
                languages_found = 0
                if l_resp.status_code == 200:
                    check_rate_limit(l_resp)
                    lang_data = l_resp.json()
                    languages_found = len(lang_data)
                    
                    for lang_name, bytes_written in lang_data.items():
                        cur.execute("""
                            INSERT INTO Languages (language_name)
                            VALUES (%s)
                            ON CONFLICT (language_name) DO NOTHING
                            RETURNING language_id
                        """, (lang_name,))
                        res = cur.fetchone()
                        if res:
                            lang_id = res[0]
                        else:
                            cur.execute("SELECT language_id FROM Languages WHERE language_name = %s", (lang_name,))
                            lang_id = cur.fetchone()[0]
                            
                        cur.execute("""
                            INSERT INTO Repository_Languages (repo_id, language_id, bytes_written)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (repo_id, language_id) DO UPDATE
                            SET bytes_written = EXCLUDED.bytes_written
                        """, (repo_id, lang_id, bytes_written))
                
                # Update last_synced_at
                cur.execute("""
                    UPDATE Repositories
                    SET last_synced_at = NOW()
                    WHERE repo_id = %s
                """, (repo_id,))
                
            conn.commit()
            print(f"Success: {commits_ingested} commits ingested, {languages_found} languages found.")
        except Exception as e:
            conn.rollback()
            print(f"Error processing {owner}/{repo}: {e}")
            
    conn.close()

if __name__ == "__main__":
    ingest()
