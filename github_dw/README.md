# GitHub Analytics Data Warehouse 📊

An end-to-end data warehouse application that ingests GitHub repository data (commits, contributors, languages) into a local PostgreSQL database, and exposes it through a clean, modern Streamlit dashboard and a Natural Language-to-SQL interface powered by Groq (LLaMA 3).

## Features
- **Idempotent Database Schema**: Fully configured PostgreSQL schema with automated tracking triggers.
- **Python Ingestion Pipeline**: Handles GitHub REST API rate limiting, robust upserts, and commit indexing.
- **Natural Language SQL querying**: Ask questions like "Who are the top 5 contributors across all repos?" and have the app safely translate it to SQL using LLaMA.
- **Beautiful Dashboard**: Component-based Streamlit UI showing top contributors, language breakdown, and inactive repos.

## Requirements
- Python 3.9+
- PostgreSQL
- GitHub Personal Access Token
- Groq API Key

## Setup & Installation

**1. Install Python dependencies:**
```bash
pip install -r requirements.txt
```

**2. Configure Environment Variables:**
Copy `.env.example` to `.env` (which is gitignored) and fill in your details:
```bash
cp .env.example .env
```

**3. Setup PostgreSQL Database:**
```bash
createdb github_dw -U postgres
psql -d github_dw -U postgres -f schema.sql
```

**4. Run App:**
```bash
# Sync data first
python ingest.py

# Launch UI
streamlit run app.py
```
