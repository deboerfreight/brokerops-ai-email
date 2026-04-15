"""Brave contact search for top hotshot candidates — Tampa FL + Seguin TX."""
from __future__ import annotations

import json
import re
import time
from contextlib import closing
from pathlib import Path

import httpx
from cryptography.fernet import Fernet
import sqlite3

# ── Vault key load ────────────────────────────────────────────────────────────
VAULT_DB = Path("C:/Users/Owner/Desktop/Claude Work/team/org/org.db")
VAULT_KEY_FILE = Path("C:/Users/Owner/Desktop/Claude Work/team/org/.vault_key")

fk = VAULT_KEY_FILE.read_bytes().strip()
fernet = Fernet(fk)
with closing(sqlite3.connect(VAULT_DB)) as conn:
    row = conn.execute(
        "SELECT encrypted_value FROM vault WHERE key_name='BRAVE_SEARCH_API_KEY'"
    ).fetchone()
    BRAVE_KEY = fernet.decrypt(row[0]).decode().strip()

print(f"Brave key loaded (len={len(BRAVE_KEY)})")

BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}")
brave_calls = 0
BUDGET = 50


def brave_contact_search(name: str, state: str, city: str = "") -> dict:
    global brave_calls
    if brave_calls >= BUDGET:
        return {"email": None, "url": "", "error": "budget_exhausted"}
    query = f"{name} {city} {state} trucking contact"
    try:
        resp = httpx.get(
            BRAVE_URL,
            params={"q": query, "count": 10},
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "X-Subscription-Token": BRAVE_KEY,
            },
            timeout=15,
        )
        brave_calls += 1
        if resp.status_code == 429:
            print(f"  429 rate limit — sleeping 6s")
            time.sleep(6)
            resp = httpx.get(
                BRAVE_URL,
                params={"q": query, "count": 10},
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": BRAVE_KEY,
                },
                timeout=15,
            )
            brave_calls += 1
        time.sleep(6)
        if resp.status_code != 200:
            return {"email": None, "url": "", "error": f"HTTP {resp.status_code}"}
        data = resp.json()
        results = (data.get("web") or {}).get("results", [])
        for item in results:
            text = (item.get("description") or "") + " " + (item.get("url") or "")
            emails = EMAIL_RE.findall(text)
            if emails:
                return {"email": emails[0], "url": item.get("url", "")[:120]}
        url_found = results[0].get("url", "") if results else ""
        return {"email": None, "url": url_found[:120]}
    except Exception as exc:
        time.sleep(6)
        return {"email": None, "url": "", "error": str(exc)}


# Top name-confirmed hotshot carriers within 60mi of Tampa + Seguin
CANDIDATES = [
    # Tampa FL
    {"dot": "02497259", "name": "VIP EXPEDITED INC",            "city": "TAMPA",         "state": "FL", "dist": 5.1,  "origin": "Tampa, FL"},
    {"dot": "04383662", "name": "BW TRUCKING HOTSHOT LLC",       "city": "PLANT CITY",    "state": "FL", "dist": 20.0, "origin": "Tampa, FL"},
    {"dot": "04046677", "name": "DSH HOTSHOT LOGISTICS LLC",     "city": "RIVERVIEW",     "state": "FL", "dist": 15.1, "origin": "Tampa, FL"},
    {"dot": "04445483", "name": "HOTSHOTZ BODYSHOP LLC",         "city": "LUTZ",          "state": "FL", "dist": 14.1, "origin": "Tampa, FL"},
    {"dot": "04486534", "name": "LEGACY LINE LOGISTICS LLC",     "city": "RUSKIN",        "state": "FL", "dist": 17.3, "origin": "Tampa, FL"},
    {"dot": "02258701", "name": "BREYON TRANSPORT SERVICES LLC", "city": "TAMPA",         "state": "FL", "dist": 1.8,  "origin": "Tampa, FL"},
    {"dot": "04492137", "name": "CACERES HOT SHOT SERVICES LLC", "city": "PLANT CITY",    "state": "FL", "dist": 20.0, "origin": "Tampa, FL"},
    {"dot": "03884061", "name": "SUNSET HOT SHOT TRANSPORT LLC", "city": "PORT RICHEY",   "state": "FL", "dist": 28.1, "origin": "Tampa, FL"},
    {"dot": "04546971", "name": "BANES HOTSHOT LOGISTICS LLC",   "city": "HOLIDAY",       "state": "FL", "dist": 23.4, "origin": "Tampa, FL"},
    # Seguin TX
    {"dot": "03408166", "name": "JSL HOTSHOTS LLC",              "city": "SEGUIN",        "state": "TX", "dist": 0.5,  "origin": "Seguin, TX"},
    {"dot": "04465478", "name": "THE LONESTAR HOT SHOT TRANSPORT LLC", "city": "NEW BRAUNFELS", "state": "TX", "dist": 12.5, "origin": "Seguin, TX"},
    {"dot": "04310020", "name": "JACKPOT HOTSHOT INC",           "city": "SMILEY",        "state": "TX", "dist": 29.4, "origin": "Seguin, TX"},
    {"dot": "02500125", "name": "ROYAL EXPEDITED LLC",           "city": "CONVERSE",      "state": "TX", "dist": 18.8, "origin": "Seguin, TX"},
    {"dot": "04431155", "name": "HOUSTONS HOTSHOT AND HAULING LLC", "city": "SAN ANTONIO","state": "TX", "dist": 30.1, "origin": "Seguin, TX"},
    {"dot": "03600800", "name": "BELLY BOYS HOTSHOTS LLC",       "city": "SAN ANTONIO",   "state": "TX", "dist": 36.4, "origin": "Seguin, TX"},
    {"dot": "04424582", "name": "ARK EXPEDITE LLC",              "city": "CONVERSE",      "state": "TX", "dist": 22.3, "origin": "Seguin, TX"},
    {"dot": "03942814", "name": "CASTLE LINE HOT SHOTS LLC",     "city": "SAN ANTONIO",   "state": "TX", "dist": 31.2, "origin": "Seguin, TX"},
    {"dot": "01790469", "name": "DAVID LEE CONN JRS HOTSHOT SERVICE LLC", "city": "FLORESVILLE", "state": "TX", "dist": 19.2, "origin": "Seguin, TX"},
]

results = {}
for c in CANDIDATES:
    print(f"Brave: {c['name']} ({c['city']}, {c['state']})...")
    r = brave_contact_search(c["name"], c["state"], c["city"])
    results[c["dot"]] = r
    print(f"  calls={brave_calls} | email={r.get('email')} | url={r.get('url', '')[:50]}")
    if brave_calls >= BUDGET:
        print("Budget reached, stopping.")
        break

print(f"\nTotal Brave calls: {brave_calls}")
print("\n=== Emails found ===")
for dot, r in results.items():
    if r and r.get("email"):
        cname = next((c["name"] for c in CANDIDATES if c["dot"] == dot), dot)
        print(f"  DOT {dot} {cname}: {r['email']}")

out_path = Path("C:/Users/Owner/brokerops-ai/scripts/logs/brave_hotshot_contacts_20260415.json")
out_path.write_text(json.dumps({"calls": brave_calls, "results": results}, indent=2))
print(f"\nSaved: {out_path}")
