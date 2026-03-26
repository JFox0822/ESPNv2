#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Data Fetcher (espn-api version)
Uses the espn-api library for robust auth handling.
"""

import json
import os
import sys
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
LEAGUE_ID = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
ESPN_S2   = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")
SEASON    = 2026

STAT_MAP = {
    "20": "R", "21": "RBI", "5": "HR", "23": "SB", "27": "Kbat",
    "2": "AVG", "17": "OPS", "34": "IP", "41": "H", "48": "K",
    "63": "QS", "47": "ERA", "53": "WHIP", "57": "SV", "83": "HLD",
}

# ESPN stat ID → fantasy category label for box score line scores
# These map the lineScore stat IDs returned by box_scores()
BOX_STAT_MAP = {
    "20": "R",   "21": "RBI",  "5": "HR",   "23": "SB",
    "27": "K",   "2": "AVG",   "17": "OPS", "34": "IP",
    "41": "H",   "48": "K",    "63": "QS",  "47": "ERA",
    "53": "WHIP","57": "SV",   "83": "HLD",
}

SLOT_MAP = {
    0:"C", 1:"1B", 2:"2B", 3:"3B", 4:"SS", 5:"OF", 6:"2B/SS",
    7:"1B/3B", 8:"LF", 9:"CF", 10:"RF", 11:"DH", 12:"UTIL",
    13:"SP", 14:"RP", 15:"P", 16:"BE", 17:"IL", 18:"IL10",
    19:"IL60", 20:"NA", 21:"BE", 22:"IL",
}

POS_MAP = {
    1:"C", 2:"1B", 3:"2B", 4:"3B", 5:"SS", 6:"OF",
    7:"2B/SS", 8:"1B/3B", 9:"P", 10:"SP", 11:"RP", 12:"DH",
}

LOWER_BETTER = {"ERA", "WHIP", "H"}

# ── Helpers ────────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

def parse_line_score(box_side):
    """Extract per-category stats and win/loss from a box score side."""
    cats = {}
    try:
        # espn-api stores line scores in different attributes depending on version
        line_scores = (
            getattr(box_side, "lineup", None) or
            getattr(box_side, "stats", None) or
            {}
        )
        # Try the lineScore dict directly on the box object
        raw = getattr(box_side, "lineScore", getattr(box_side, "line_score", {})) or {}
        if isinstance(raw, dict):
            for stat_id, val in raw.items():
                lbl = BOX_STAT_MAP.get(str(stat_id))
                if lbl and val is not None:
                    cats[lbl] = {"value": round(float(val), 3) if isinstance(val, float) else val}
    except Exception:
        pass
    return cats

def extract_box_categories(b):
    """
    Extract per-category stats + win/loss for home and away from a box score.
    espn-api box score objects have home_stats and away_stats dicts.
    """
    home_cats = {}
    away_cats = {}

    try:
        # Method 1: home_stats / away_stats dicts (keyed by stat ID)
        h_stats = getattr(b, "home_stats", {}) or {}
        a_stats = getattr(b, "away_stats", {}) or {}

        if h_stats and a_stats:
            for stat_id, lbl in BOX_STAT_MAP.items():
                hv = h_stats.get(int(stat_id), h_stats.get(stat_id))
                av = a_stats.get(int(stat_id), a_stats.get(stat_id))
                if hv is None and av is None:
                    continue
                hval = hv.get("value", hv) if isinstance(hv, dict) else hv
                aval = av.get("value", av) if isinstance(av, dict) else av
                if hval is None and aval is None:
                    continue
                # Determine who won this category
                try:
                    hf, af = float(hval or 0), float(aval or 0)
                    lower = lbl in LOWER_BETTER
                    if hf == af:
                        h_res, a_res = "TIE", "TIE"
                    elif (hf < af) == lower:  # lower is better and home is lower → home wins
                        h_res, a_res = "WIN", "LOSS"
                    else:
                        h_res, a_res = "LOSS", "WIN"
                except Exception:
                    h_res, a_res = "TIE", "TIE"

                fmt = lambda v, l: (
                    f"{float(v):.3f}" if l in {"AVG","OPS","ERA","WHIP"} and v is not None
                    else (f"{float(v):.1f}" if l == "IP" and v is not None
                    else str(v) if v is not None else "—")
                )
                home_cats[lbl] = {"value": fmt(hval, lbl), "result": h_res}
                away_cats[lbl] = {"value": fmt(aval, lbl), "result": a_res}
    except Exception as e:
        pass

    # Method 2: Try accessing via scoring period stats on each team
    if not home_cats:
        try:
            for side, cats_out in [(b.home_team, home_cats), (b.away_team, away_cats)]:
                if side is None:
                    continue
                period_stats = {}
                # espn-api stores current week stats in different places
                for attr in ["stats", "season_stats", "currentPeriodStats"]:
                    v = getattr(side, attr, None)
                    if v and isinstance(v, dict):
                        period_stats = v
                        break
                for stat_id, lbl in BOX_STAT_MAP.items():
                    val = period_stats.get(int(stat_id), period_stats.get(stat_id))
                    if val is not None:
                        v = val.get("value", val) if isinstance(val, dict) else val
                        cats_out[lbl] = {"value": v, "result": "TIE"}
        except Exception:
            pass

    return home_cats, away_cats

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    try:
        from espn_api.baseball import League
    except ImportError:
        print("❌  espn-api not installed. Run: pip install espn-api", file=sys.stderr)
        sys.exit(1)

    print(f"🔄  Connecting to ESPN league {LEAGUE_ID}, season {SEASON} …")

    kwargs = {"league_id": LEAGUE_ID, "year": SEASON}
    if ESPN_S2 and ESPN_SWID:
        kwargs["espn_s2"] = ESPN_S2
        kwargs["swid"]    = ESPN_SWID
        print("  🔐  Using ESPN_S2 + SWID cookies")
    else:
        print("  ⚠️   No cookies — fetching as public league")

    try:
        league = League(**kwargs)
    except Exception as e:
        print(f"❌  Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    updated = now_utc()
    current_week = league.current_week
    print(f"  📅  Current week: {current_week}")

    # ── 1. Team map ─────────────────────────────────────────────────────────────
    team_map = {}
    for t in league.teams:
        raw_owners = getattr(t, "owners", []) or []
        owners = [o.get("displayName", o.get("firstName","?")) if isinstance(o, dict) else str(o) for o in raw_owners]
        team_map[t.team_id] = {
            "id":     t.team_id,
            "name":   t.team_name,
            "abbrev": getattr(t, "team_abbrev", t.team_name[:3].upper()),
            "owners": owners,
            "logo":   getattr(t, "logo_url", getattr(t, "logo", "")),
        }

    # ── 2. Standings ─────────────────────────────────────────────────────────────
    standings = []
    for t in league.teams:
        tm = team_map[t.team_id]
        standings.append({
            **tm,
            "wins":          t.wins,
            "losses":        t.losses,
            "ties":          t.ties,
            "pointsFor":     round(getattr(t, "points_for", 0) or 0, 1),
            "pointsAgainst": round(getattr(t, "points_against", 0) or 0, 1),
            "streak":        getattr(t, "streak_length", 0),
            "streakType":    getattr(t, "streak_type", ""),
            "seed":          getattr(t, "playoff_pct", 0),
        })

    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1

    save("standings.json", {"week": current_week, "standings": standings, "updated": updated})

    # ── 3. Matchups (with full category stats) ───────────────────────────────────
    matchups_out = []
    try:
        box_scores = league.box_scores(current_week)
        for b in box_scores:
            home_cats, away_cats = extract_box_categories(b)

            def parse_side(team, cat_wins, cat_losses, cat_ties, cats):
                if team is None:
                    return {"teamId": None, "team": "BYE", "abbrev": "BYE",
                            "catWins": 0, "catLoss": 0, "catTies": 0, "categories": {}}
                tm = team_map.get(team.team_id, {})
                return {
                    "teamId":     team.team_id,
                    "team":       team.team_name,
                    "abbrev":     tm.get("abbrev", ""),
                    "catWins":    cat_wins,
                    "catLoss":    cat_losses,
                    "catTies":    cat_ties,
                    "categories": cats,
                }

            home_side = parse_side(b.home_team, b.home_wins, b.home_losses, b.home_ties, home_cats)
            away_side = parse_side(b.away_team, b.away_wins, b.away_losses, b.away_ties, away_cats)
            hw = home_side["catWins"] > away_side["catWins"]
            aw = away_side["catWins"] > home_side["catWins"]
            matchups_out.append({
                "home":   home_side,
                "away":   away_side,
                "leader": home_side["team"] if hw else (away_side["team"] if aw else "Tied"),
                "winner": "UNDECIDED",
            })
        print(f"  📊  {len(matchups_out)} matchups fetched")
    except Exception as e:
        print(f"  ⚠️   Box scores unavailable: {e}", file=sys.stderr)

    save("matchups.json", {
        "week":     current_week,
        "period":   current_week,
        "matchups": matchups_out,
        "updated":  updated,
    })

    # ── 4. Team stats ────────────────────────────────────────────────────────────
    team_stats = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        stats = {}
        raw_stats = getattr(t, "stats", {}) or getattr(t, "valuesByStat", {}) or {}
        for k, v in raw_stats.items():
            lbl = STAT_MAP.get(str(k))
            if lbl:
                stats[lbl] = round(v, 3) if isinstance(v, float) else v
        team_stats.append({
            **tm,
            "wins":   s_rec.get("wins", 0),
            "losses": s_rec.get("losses", 0),
            "stats":  stats,
        })
    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── 5. Rosters ───────────────────────────────────────────────────────────────
    rosters_out = []
    for t in league.teams:
        tm = team_map[t.team_id]
        players = []
        for p in (t.roster or []):
            slot_id    = getattr(p, "lineupSlot", getattr(p, "slot_id", 16))
            slot_label = SLOT_MAP.get(slot_id, "BE") if isinstance(slot_id, int) else str(slot_id)
            pos_id     = getattr(p, "defaultPositionId", getattr(p, "position_id", 0)) or 0
            primary_pos = POS_MAP.get(pos_id, "?")
            eligible_ids = getattr(p, "eligibleSlots", []) or []
            eligible_pos = list(dict.fromkeys(
                SLOT_MAP.get(s, "") for s in eligible_ids
                if SLOT_MAP.get(s, "") not in ("BE","IL","IL10","IL60","NA","")
            ))
            eligible_str = "/".join(eligible_pos)[:20]
            is_pitcher   = pos_id in (9, 10, 11, 13)
            inj_status   = getattr(p, "injuryStatus", "ACTIVE") or "ACTIVE"
            stats = {}
            for k, v in (getattr(p, "stats", {}) or {}).items():
                lbl = STAT_MAP.get(str(k))
                if lbl:
                    stats[lbl] = round(v, 3) if isinstance(v, float) else v

            acq_type    = (getattr(p, "acquisitionType", "") or "").upper()
            draft_round = getattr(p, "draftRound", 0) or 0
            # Keeper eligible: drafted Rd 13+ OR picked up as FA/Waiver
            is_keeper_elig = (draft_round >= 13) or (draft_round == 0 and any(
                x in acq_type for x in ["FREE", "WAIVER", "FA"]
            ))

            players.append({
                "name":            getattr(p, "name", "Unknown"),
                "slot":            slot_label,
                "position":        primary_pos,
                "eligible":        eligible_str,
                "isPitcher":       is_pitcher,
                "injStatus":       inj_status,
                "stats":           stats,
                "tier":            "",
                "acquisitionType": acq_type,
                "draftRound":      draft_round,
                "keeperEligible":  is_keeper_elig,
            })

        rosters_out.append({**tm, "teamId": t.team_id, "players": players})

    save("rosters.json", {
        "season":    SEASON,
        "week":      current_week,
        "teams":     rosters_out,
        "projBlend": {"projPct": 0, "actualPct": 100},
        "updated":   updated,
    })

    # ── 6. Power Rankings ────────────────────────────────────────────────────────
    ts_map = {t["id"]: t.get("stats", {}) for t in team_stats}
    power  = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        power.append({
            "id":          t.team_id,
            "name":        t.team_name,
            "abbrev":      tm.get("abbrev", ""),
            "overallW":    s_rec.get("wins", 0),
            "overallL":    s_rec.get("losses", 0),
            "overallRank": s_rec.get("rank", 0),
            "h2hWins":     s_rec.get("wins", 0),
            "h2hLosses":   s_rec.get("losses", 0),
            "pwWins":      0,
            "pwLosses":    0,
            "composite":   0.0,
            "catRanks":    {},
        })

    all_cats = ["R","RBI","HR","SB","AVG","OPS","IP","H","K","QS","ERA","WHIP","SV","HLD"]
    for cat in all_cats:
        vals = [(p["id"], ts_map.get(p["id"], {}).get(cat)) for p in power]
        vals = [(pid, v) for pid, v in vals if v is not None]
        if len(vals) < 2:
            continue
        vals.sort(key=lambda x: x[1], reverse=(cat not in LOWER_BETTER))
        for rank, (pid, _) in enumerate(vals, 1):
            for p in power:
                if p["id"] == pid:
                    p["catRanks"][cat] = rank

    power.sort(key=lambda x: (-x["overallW"], x["overallL"]))
    for i, p in enumerate(power):
        p["pwRank"]    = i + 1
        p["rankDelta"] = 0

    save("power_rankings.json", {"week": current_week, "rankings": power, "updated": updated})

    # ── 7. Meta ──────────────────────────────────────────────────────────────────
    team_list = [{"id": t["id"], "name": t["name"], "abbrev": t["abbrev"]} for t in standings]
    save("meta.json", {
        "leagueName":    getattr(league.settings, "name", "The League"),
        "season":        SEASON,
        "currentWeek":   current_week,
        "scoringPeriod": current_week,
        "teamCount":     len(league.teams),
        "teams":         team_list,
        "updated":       updated,
    })

    # ── 8. KOH ─────────────────────────────────────────────────────────────────
    koh_state = update_koh(league, current_week, team_map, updated)
    save("koh.json", koh_state)

    # ── 9. Draft + Keepers ───────────────────────────────────────────────────
    draft_picks, keeper_by_name = fetch_draft_and_keepers(league, team_map)
    save("draft.json", {
        "season":         SEASON,
        "picks":          draft_picks,
        "keeperEligible": keeper_by_name,
        "updated":        updated,
    })

    print(f"\n🏆  Done! Week {current_week}.")


# ── KOH (King of the Hill) ─────────────────────────────────────────────────
def update_koh(league, current_week, team_map, updated):
    koh_path = "data/koh.json"
    all_names = {tid: tm["name"] for tid, tm in team_map.items()}

    if os.path.exists(koh_path):
        with open(koh_path) as f:
            state = json.load(f)
        if state.get("season") != SEASON:
            state = None
    else:
        state = None

    if state is None:
        state = {
            "season":         SEASON,
            "active":         list(team_map.keys()),
            "eliminated":     [],
            "champions":      [],
            "currentRound":   1,
            "processedWeeks": [],
            "history":        [],
            "teamNames":      all_names,
        }

    for week in range(1, current_week):
        if week in state["processedWeeks"]:
            continue
        try:
            boxes = league.box_scores(week)
        except Exception as e:
            print(f"  ⚠️  KOH: could not get week {week} box scores: {e}")
            continue

        all_scores = {}
        losers = []
        for b in boxes:
            ht, at = b.home_team, b.away_team
            if not ht or not at:
                continue
            hid, aid = ht.team_id, at.team_id
            hw = getattr(b, 'home_wins', 0) or 0
            aw = getattr(b, 'away_wins', 0) or 0
            all_scores[hid] = hw
            all_scores[aid] = aw
            if hw < aw:
                losers.append({"teamId": hid, "team": all_names.get(hid, f"Team {hid}"), "catWins": hw})
            elif aw < hw:
                losers.append({"teamId": aid, "team": all_names.get(aid, f"Team {aid}"), "catWins": aw})

        active_set    = set(state["active"])
        active_losers = [l for l in losers if l["teamId"] in active_set]

        week_entry = {
            "week":    week,
            "round":   state["currentRound"],
            "allScores": {str(k): v for k, v in all_scores.items()},
            "losers":  [{"team": l["team"], "catWins": l["catWins"]} for l in losers],
            "eliminated": [],
            "allActive":  {all_names.get(tid, str(tid)): True for tid in state["active"]},
        }

        if active_losers:
            min_cats = min(l["catWins"] for l in active_losers)
            to_elim  = [l for l in active_losers if l["catWins"] == min_cats]
            for t in to_elim:
                if t["teamId"] in state["active"]:
                    state["active"].remove(t["teamId"])
                e_entry = {"teamId": t["teamId"], "team": t["team"],
                           "week": week, "catWins": t["catWins"], "round": state["currentRound"]}
                state["eliminated"].append(e_entry)
                week_entry["eliminated"].append(e_entry)

            week_entry["allActive"] = {
                all_names.get(tid, str(tid)): tid in state["active"] for tid in team_map
            }

            if len(state["active"]) == 1:
                winner_id   = state["active"][0]
                winner_name = all_names.get(winner_id, f"Team {winner_id}")
                state["champions"].append({"teamId": winner_id, "team": winner_name,
                                           "week": week, "round": state["currentRound"]})
                week_entry["champion"] = winner_name
                state["active"] = list(team_map.keys())
                state["currentRound"] += 1
            elif len(state["active"]) == 0:
                state["active"] = list(team_map.keys())
                state["currentRound"] += 1

        state["processedWeeks"].append(week)
        state["history"].append(week_entry)

    state["updated"] = updated
    return state


# ── Draft + Keepers ────────────────────────────────────────────────────────
def fetch_draft_and_keepers(league, team_map):
    draft_picks     = []
    keeper_eligible = {}

    try:
        for pick in (league.draft or []):
            try:
                team_id     = pick.team.team_id if hasattr(pick, 'team') and pick.team else None
                player_name = getattr(pick, 'playerName', None) or getattr(pick, 'player_name', 'Unknown')
                round_num   = getattr(pick, 'round_num', 0) or 0
                round_pick  = getattr(pick, 'round_pick', 0) or 0
                team_name   = team_map.get(team_id, {}).get("name", "?") if team_id else "?"
                keeper_elig = round_num >= 13

                draft_picks.append({
                    "round":         round_num,
                    "pick":          round_pick,
                    "overall":       (round_num - 1) * len(team_map) + round_pick,
                    "teamId":        team_id,
                    "team":          team_name,
                    "player":        player_name,
                    "keeperEligible": keeper_elig,
                })
                if keeper_elig and team_id:
                    keeper_eligible.setdefault(team_id, [])
                    keeper_eligible[team_id].append({"name": player_name, "source": f"Rd {round_num}"})
            except Exception as pe:
                print(f"  ⚠️  Draft pick error: {pe}")
    except Exception as e:
        print(f"  ⚠️  Draft data unavailable: {e}")

    # FA/Waiver pickups from rosters
    try:
        for t in league.teams:
            for player in (t.roster or []):
                try:
                    acq = getattr(player, 'acquisitionType', '') or ''
                    if any(x in acq.upper() for x in ['FREE', 'WAIVER', 'FA']):
                        keeper_eligible.setdefault(t.team_id, [])
                        pname = getattr(player, 'name', 'Unknown')
                        if not any(p['name'] == pname for p in keeper_eligible[t.team_id]):
                            keeper_eligible[t.team_id].append({"name": pname, "source": "FA/Waiver"})
                except Exception:
                    pass
    except Exception as e:
        print(f"  ⚠️  FA acquisition data: {e}")

    keeper_by_name = {
        team_map.get(tid, {}).get("name", f"Team {tid}"): players
        for tid, players in keeper_eligible.items()
    }
    return draft_picks, keeper_by_name


if __name__ == "__main__":
    main()
