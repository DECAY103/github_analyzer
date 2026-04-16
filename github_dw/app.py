import os
import psycopg2
import pandas as pd
import streamlit as st
import subprocess
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "github_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

st.set_page_config(page_title="GitHub Analytics DW", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
/* App background color */
.stApp {
    background-color: #f7f9fc;
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background-color: #ffffff;
    border-right: 1px solid #e1e4e8;
}

/* Main title styling */
.main-title {
    font-size: 32px;
    font-weight: 700;
    color: #1a1e23;
    margin-bottom: 24px;
}

/* Card Styling */
div.css-1r6slb0, div.css-12oz5g7 {
    background-color: #ffffff;
    padding: 24px;
    border-radius: 16px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
}
[data-testid="stMetric"] {
    background-color: #ffffff;
    padding: 24px;
    border-radius: 16px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.03);
    border: 1px solid #f0f2f5;
}

/* Hide some default artifacts */
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Connect to DB
@st.cache_resource
def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

conn = get_db_connection()

def run_query(query, params=None):
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        conn.rollback()
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

# Layout
with st.sidebar:
    st.markdown("### 💠 GitHub DW")
    st.markdown("<br>", unsafe_allow_html=True)
    
    page = st.radio("Navigation", ["Dashboard", "SQL Explorer", "Ask AI", "Internals"], label_visibility="collapsed")
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    st.markdown("#### Admin Actions")
    if st.button("🔄 Sync Live Data", use_container_width=True):
        with st.spinner("Running ingest.py..."):
            result = subprocess.run(["python", "ingest.py"], capture_output=True, text=True)
            if result.returncode == 0:
                st.sidebar.success("Sync complete!")
            else:
                st.sidebar.error("Sync failed!")
                st.sidebar.text(result.stderr)

st.markdown(f'<div class="main-title">{page} overview</div>', unsafe_allow_html=True)

if page == "Dashboard":
    repo_cnt = run_query("SELECT COUNT(*) as c FROM Repositories").iloc[0]["c"] if conn else 0
    commit_cnt = run_query("SELECT COUNT(*) as c FROM Commits").iloc[0]["c"] if conn else 0
    contrib_cnt = run_query("SELECT COUNT(*) as c FROM Users").iloc[0]["c"] if conn else 0
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Repositories", repo_cnt, "Active tracking")
    c2.metric("Total Commits", commit_cnt, "Lifetime volume")
    c3.metric("Total Contributors", contrib_cnt, "Unique users")
    
    st.markdown("<br>", unsafe_allow_html=True)
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.write("#### Top 10 Contributors")
        df_top = run_query("SELECT * FROM get_top_contributors(10)")
        if not df_top.empty:
            st.bar_chart(df_top.set_index("username")["total_commits"])
        else:
            st.info("No contributor data available.")
            
    with col_b:
        st.write("#### Language Breakdown")
        df_lang = run_query("SELECT * FROM get_language_breakdown()")
        if not df_lang.empty:
            st.bar_chart(df_lang.set_index("language")["total_bytes"])
        else:
            st.info("No language data available.")
        
    st.markdown("<br>", unsafe_allow_html=True)
    st.write("#### Inactive Repositories (>30 days)")
    df_inactive = run_query("SELECT * FROM get_inactive_repos(30)")
    if not df_inactive.empty:
        st.dataframe(df_inactive, use_container_width=True)
    else:
        st.info("No inactive repositories found.")

elif page == "SQL Explorer":
    st.write("Run direct PostgreSQL queries against the warehouse.")
    default_query = "SELECT * FROM cross_repo_activity LIMIT 20;"
    query = st.text_area("SQL Statement", value=default_query, height=150)
    if st.button("Execute Query", type="primary"):
        res_df = run_query(query)
        if not res_df.empty:
            st.dataframe(res_df, use_container_width=True)
        else:
            st.info("Execution complete. (0 rows returned)")

elif page == "Ask AI":
    if "nl_query" not in st.session_state:
        st.session_state.nl_query = ""
    if "run_nl" not in st.session_state:
        st.session_state.run_nl = False
        
    def set_question(q):
        st.session_state.nl_query = q
        st.session_state.run_nl = True
        
    st.write("Ask natural language questions about your tracking data.")
    c1, c2, c3 = st.columns(3)
    c1.button("Top 5 contributors?", on_click=set_question, args=("Who are the top 5 contributors across all repos?",), use_container_width=True)
    c2.button("Inactive repos (>30d)?", on_click=set_question, args=("Which repos have been inactive for 30 days?",), use_container_width=True)
    c3.button("Dominant languages?", on_click=set_question, args=("What languages dominate the codebase?",), use_container_width=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    nl_input = st.text_input("What do you want to know?", value=st.session_state.nl_query)
    
    submitted = st.button("Ask Groq AI", type="primary")
    
    if submitted or st.session_state.run_nl or (nl_input and nl_input != st.session_state.get("last_asked")):
        if nl_input:
            st.session_state.last_asked = nl_input
            st.session_state.run_nl = False
            
            if not GROQ_API_KEY:
                st.error("Missing GROQ_API_KEY in environment.")
            else:
                client = Groq(api_key=GROQ_API_KEY)
                
                prompt = f"Question: {nl_input}"
                system_prompt = """You are a PostgreSQL expert assistant for a GitHub Analytics Data Warehouse.
Convert the user's question into a valid PostgreSQL query. Return ONLY the SQL query — no explanation, no markdown, no backticks.

Database schema:
- Users(user_id PK, github_username UNIQUE, name, email)
- Repositories(repo_id PK, repo_name, owner_id FK→Users, description, last_synced_at)
- Languages(language_id PK, language_name UNIQUE)
- Repository_Languages(repo_id FK, language_id FK, bytes_written) — composite PK
- Commits(commit_sha PK, repo_id FK, author_id FK→Users, message, commit_timestamp, lines_added, lines_deleted)
- Contributor_Summary(user_id FK, repo_id FK, total_commits, latest_commit_date) — composite PK, maintained by trigger

Useful views:
- monthly_commit_trend — commit counts per repo per month with delta vs previous month
- rising_contributors — users whose first commit was within 60 days and have 5+ commits
- cross_repo_activity — contributor commit counts per repo with rank within that repo
- inactive_repos — repos with no commits in 30+ days

Useful stored procedures (call with SELECT * FROM ...):
- get_top_contributors(limit_n INT) — global leaderboard
- get_language_breakdown() — languages ranked by total bytes
- get_inactive_repos(days_threshold INT) — parameterized inactivity check

Return only valid PostgreSQL. Do not use any tables or columns not listed above."""
                
                try:
                    with st.spinner("Asking Groq..."):
                        chat_completion = client.chat.completions.create(
                            messages=[
                                {
                                    "role": "system",
                                    "content": system_prompt,
                                },
                                {
                                    "role": "user",
                                    "content": prompt,
                                }
                            ],
                            model="llama-3.3-70b-versatile",
                        )
                        sql_query = chat_completion.choices[0].message.content.replace("```sql", "").replace("```", "").strip()
                        
                    st.code(sql_query, language="sql")
                    
                    df_nl = run_query(sql_query)
                    if not df_nl.empty:
                        st.dataframe(df_nl, use_container_width=True)
                    else:
                        st.info("Query executed successfully but returned no results.")
                except Exception as e:
                    st.error(f"Error calling Groq or executing query: {e}")

elif page == "Internals":
    st.write("Demonstration of Database Backend Features")
    
    st.markdown("#### 1. Real-time Trigger Demo")
    if st.button("Insert Test Commit"):
        if conn:
            import uuid
            import random
            from datetime import datetime
            dummy_sha = str(uuid.uuid4()).replace("-", "")[:40]
            
            cur = conn.cursor()
            cur.execute("SELECT repo_id FROM Repositories LIMIT 1")
            repo_res = cur.fetchone()
            cur.execute("SELECT user_id FROM Users LIMIT 1")
            user_res = cur.fetchone()
            
            if repo_res and user_res:
                repo_id = repo_res[0]
                user_id = user_res[0]
                
                try:
                    cur.execute("""
                        INSERT INTO Commits (commit_sha, repo_id, author_id, message, commit_timestamp)
                        VALUES (%s, %s, %s, 'Dummy test commit', %s)
                    """, (dummy_sha, repo_id, user_id, datetime.now()))
                    conn.commit()
                    
                    st.success(f"Inserted ({dummy_sha[:8]}) -> Triggers updated Contributor_Summary directly!")
                    df_trig = run_query(f"SELECT * FROM Contributor_Summary WHERE user_id = {user_id} AND repo_id = {repo_id}")
                    st.dataframe(df_trig)
                except Exception as e:
                    conn.rollback()
                    st.error(f"Error inserting commit: {e}")
            else:
                st.warning("Please sync at least once so there's a repository and user to attach the commit to.")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("#### 2. Query Optimization (EXPLAIN ANALYZE)")
    explain_query_map = {
        "monthly trend": "SELECT * FROM monthly_commit_trend",
        "top contributors": "SELECT * FROM get_top_contributors(10)",
        "inactive repos": "SELECT * FROM inactive_repos"
    }
    opt = st.selectbox("Select View/Procedure", list(explain_query_map.keys()))
    if st.button("Analyze Query"):
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("EXPLAIN ANALYZE " + explain_query_map[opt])
                explain_output = "\\n".join([row[0] for row in cur.fetchall()])
                conn.rollback()
                st.code(explain_output, language="sql")
            except Exception as e:
                conn.rollback()
                st.error(f"Error: {e}")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("#### 3. ACID Compliance Demo")
    if st.button("Simulate Rollback Event"):
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM Repositories")
                before_repo = cur.fetchone()[0]
                
                # Bad PK
                cur.execute("INSERT INTO Users (user_id, github_username) VALUES (9999999, 'acid_test_user')")
                cur.execute("INSERT INTO Repositories (repo_id, repo_name, owner_id) VALUES (9999999, 'acid_test_repo', 9999999)")
                conn.rollback()
                
                cur.execute("SELECT COUNT(*) FROM Repositories")
                after_repo = cur.fetchone()[0]
                
                st.success("Transaction rolled back dynamically!")
                st.write(f"Repositories before failed insert: **{before_repo}** | Repositories after rollback: **{after_repo}**")
            except Exception as e:
                conn.rollback()
                st.error(f"Error: {e}")
