"""
Deploy / update Retell AI voice agents for deBoer Freight.

Three-agent phone system on 866-926-HAUL:
  1. Directory Agent  → greets, routes to Nina or Sofia
  2. Nina Weston      → customer service (Della voice)
  3. Sofia Reyes      → carrier relations (Lily voice)

Internal routing numbers (invisible to callers):
  - Nina: +19548342835
  - Sofia: +19546271882

Run this script to recreate agents from scratch if needed.
Requires RETELL_API_KEY in .env.
"""
from __future__ import annotations

import os
from dotenv import load_dotenv
from retell import Retell

load_dotenv()
client = Retell(api_key=os.environ["RETELL_API_KEY"])

# ── Phone numbers ──────────────────────────────────────────────────────────
MAIN_LINE = "+18669264285"          # 866-926-HAUL (public)
NINA_INTERNAL = "+19548342835"      # internal routing
SOFIA_INTERNAL = "+19546271882"     # internal routing
DEREK_DIRECT = "+13057673480"       # Google Voice, fallback

# ── Agent IDs (current production) ─────────────────────────────────────────
DIRECTORY_AGENT_ID = "agent_231401e41e2507c956a614ab45"
NINA_AGENT_ID = "agent_560b072fac7b27b2cf091b932e"
SOFIA_AGENT_ID = "agent_202287e5f2b157c6722e804250"

# ── LLM IDs (current production) ──────────────────────────────────────────
DIRECTORY_LLM_ID = "llm_55d22b3fdaa3279f2b2bbc9b30e6"
NINA_LLM_ID = "llm_7959f818cd82e791394147c04b6f"
SOFIA_LLM_ID = "llm_e9ba056acc00d3547caacb2a51ad"

# ── Verify current state ──────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Retell Agent Status ===\n")

    agents = client.agent.list()
    for a in agents:
        print(f"  {a.agent_name}")
        print(f"    ID: {a.agent_id}")
        print(f"    Voice: {a.voice_id}")
        print(f"    Speed: {a.voice_speed} | Temp: {a.voice_temperature}")
        print(f"    Responsiveness: {a.responsiveness}")
        print()

    print("=== Phone Routing ===\n")
    phones = client.phone_number.list()
    for p in phones:
        nick = getattr(p, "nickname", "")
        print(f"  {p.phone_number} → {p.inbound_agent_id}")
        if nick:
            print(f"    ({nick})")
    print()

    print("=== Routing Map ===\n")
    print(f"  866-926-HAUL → Directory → Customer (1) → Nina ({NINA_INTERNAL})")
    print(f"                           → Carrier  (2) → Sofia ({SOFIA_INTERNAL})")
    print(f"                           → Derek (hidden) → {DEREK_DIRECT}")
    print(f"  Nina ↔ Sofia cross-transfer enabled")
