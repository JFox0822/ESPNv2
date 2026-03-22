# The League – ESPN Fantasy Baseball Dashboard

A GitHub Pages dashboard for a private ESPN H2H Categories fantasy baseball league.  
Data refreshes automatically every morning via GitHub Actions.

---

## Repo Structure

```
├── index.html              ← Static dashboard (GitHub Pages)
├── fetch_espn.py           ← Data fetch script (run by Actions)
├── data/
│   ├── meta.json           ← League info / timestamps
│   ├── standings.json      ← Current standings
│   ├── matchups.json       ← Current week matchups + categories
│   ├── team_stats.json     ← Season cumulative stats per team
│   └── power_rankings.json ← Power rankings (record vs. field)
└── .github/
    └── workflows/
        └── refresh.yml     ← Daily GitHub Actions workflow
```

---

## Setup

### 1. Create the repo
Create a new GitHub repo (e.g. `fantasy-baseball-dashboard`) and push these files.

### 2. Enable GitHub Pages
- **Settings → Pages → Source:** Deploy from `main` branch, root `/`
- Your dashboard will be live at `https://YOUR_USERNAME.github.io/REPO_NAME/`

### 3. Add GitHub Secrets
Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret Name      | Where to find it |
|------------------|-----------------|
| `ESPN_LEAGUE_ID` | URL when viewing your league: `?leagueId=XXXXXX` |
| `ESPN_S2`        | Browser cookie — see below |
| `ESPN_SWID`      | Browser cookie — see below |

### 4. Get ESPN cookies (for private leagues)

1. Log into [ESPN Fantasy Baseball](https://www.espn.com/fantasy/baseball/)
2. Open **DevTools → Application → Cookies → espn.com**
3. Copy the value of **`espn_s2`** and **`SWID`**

> **Note:** These cookies expire periodically. If the Action starts failing with 401/403 errors, re-grab fresh cookies and update the secrets.

### 5. Run the Action manually first
- Go to **Actions → Daily ESPN Data Refresh → Run workflow**
- This creates the `data/*.json` files and commits them
- After that, it runs automatically every morning at ~4 AM ET

---

## Scoring Categories

| Hitting            | Pitching              |
|--------------------|-----------------------|
| R, RBI, HR, SB, K  | IP, H, K, QS          |
| AVG, OPS           | ERA, WHIP, SV/HLD     |

---

## Local Testing

```bash
pip install requests

# Set env vars
export ESPN_LEAGUE_ID="your_league_id"
export ESPN_S2="your_espn_s2_cookie"
export ESPN_SWID="{your-swid-cookie}"

python fetch_espn.py

# Serve the dashboard locally
python -m http.server 8080
# Open http://localhost:8080
```

---

## Tabs

| Tab | Description |
|-----|-------------|
| **Standings** | Full league standings with W–L, PCT, PF/PA, and current streak |
| **Matchups** | Current week head-to-head matchups with category-level breakdown |
| **Team Stats** | Season cumulative hitting and pitching stats for all teams |
| **Power Rankings** | Record-vs.-everyone ranking: how each team would do vs. the whole field |

---

## Coming Soon
- Historical record book integration (when data arrives)
- Season-over-season trends
- Category rankings heat map
