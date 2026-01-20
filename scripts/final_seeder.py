import csv
import glob
import json
import os
import random
import time
from datetime import datetime

import google.generativeai as genai
from PIL import Image

# ==========================================
# 🔧 KONFIGURASI PROJECT
# ==========================================
GOOGLE_API_KEY = "MASUKKAN_API_KEY_ANDA_DISINI"

# Path File
RAW_DATA_DIR = "raw_data"
IMAGE_DIR = "data/images"
OUTPUT_JSON = "data/mock_db.json"

# --- SETTINGAN DEMO ---
GROUP_SIZE = 15  # 1 Kelompok = 15 Nasabah
MAX_NODES = 100  # Total Node yang dibuat
AI_LIMIT = 20  # 20 Node pertama pakai Real AI, sisanya Smart Mockup

# Setup Gemini
if GOOGLE_API_KEY == "MASUKKAN_API_KEY_ANDA_DISINI":
    print("⚠️  PERINGATAN: API Key belum diisi.")
else:
    genai.configure(api_key=GOOGLE_API_KEY)

model = genai.GenerativeModel("gemini-1.5-flash")

# ==========================================
# 🧠 AI PROMPT (Hanya untuk Real AI)
# ==========================================
GROUP_ANALYSIS_PROMPT = """
Role: Senior Risk Analyst Microfinance Indonesia.
Data Kelompok:
{group_text}

Tugas: Analisis profil risiko kelompok ini.

Output JSON (Strict JSON, no markdown):
{{
  "risk_badge": "LOW RISK / MED RISK / HIGH RISK",
  "trust_score": (Integer 0-100),
  "sentiment_text": "Satu kalimat ringkas bahasa Indonesia tentang sentimen kelompok.",
  "asset_condition": "GOOD / AVERAGE / POOR",
  "asset_tags": ["Tag1", "Tag2"],
  "repayment_prediction": (Integer 0-100)
}}
"""

# ==========================================
# 🛠️ SMART GENERATORS (Menjamin Struktur Lengkap)
# ==========================================


def generate_group_name(index):
    """Nama Kelompok Realistis"""
    prefix = ["Kelompok", "Paguyuban", "Koperasi", "Mitra"]
    adjective = [
        "Maju",
        "Sejahtera",
        "Makmur",
        "Sentosa",
        "Barokah",
        "Sinar",
        "Harapan",
        "Cahaya",
        "Mandiri",
        "Bersama",
    ]
    noun = [
        "Jaya",
        "Abadi",
        "Lestari",
        "Berkah",
        "Usaha",
        "Karya",
        "Bina",
        "Dana",
        "Sahabat",
        "Mitra",
    ]
    random.seed(index)
    name = f"{random.choice(prefix)} {random.choice(adjective)} {random.choice(noun)} {index}"
    random.seed(time.time())
    return name.upper()


def generate_trend_data(trust_score, is_asset=False):
    """
    🔵 SMART MOCK: Membuat array grafik yang masuk akal sesuai skor.
    """
    months = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun"]
    data = []

    # Base value logic
    current = trust_score
    if is_asset:
        current = int(
            trust_score * 0.8
        )  # Asset value biasanya sedikit di bawah repayment rate

    vals = []
    for _ in range(6):
        vals.append(current)
        # Volatilitas
        change = random.randint(-5, 5)
        # Trend logic
        if trust_score > 80:  # Trend naik
            current -= random.randint(0, 3)
        elif trust_score < 50:  # Trend turun
            current += random.randint(2, 8)

        current = max(10, min(100, current + change))

    vals.reverse()  # Urutkan Jan -> Jun

    for i, m in enumerate(months):
        data.append({"month": m, "value" if is_asset else "rate": vals[i]})

    return data


def generate_risk_members(count, risk_type):
    """🔵 MOCK: Daftar anggota untuk popup (Wajib ada biar FE aman)"""
    names = [
        "Sri",
        "Budi",
        "Siti",
        "Agus",
        "Dewi",
        "Rina",
        "Joko",
        "Wati",
        "Endang",
        "Eko",
    ]
    members = []

    # Generate 3-5 sample member
    for i in range(random.randint(3, 5)):
        r_score = (
            random.randint(10, 30) if risk_type == "healthy" else random.randint(40, 90)
        )
        members.append(
            {
                "name": f"Ibu {random.choice(names)}",
                "risk_score": f"{r_score}%",
                "hops": f"{random.randint(1, 2)} hop",
                "role": "Anggota",
            }
        )
    return members


# ==========================================
# 🚀 DATA PROCESSOR
# ==========================================
def process_data():
    print(f"🚀 MEMULAI SEEDING FULL STRUCTURE ({MAX_NODES} Nodes)...")

    # 1. Load CSV
    def load_csv_safe(name):
        path = os.path.join(RAW_DATA_DIR, name)
        if not os.path.exists(path):
            return []
        try:
            with open(path, mode="r", encoding="utf-8-sig") as f:
                sample = f.readline()
                delim = ";" if ";" in sample else ","
                f.seek(0)
                return list(csv.DictReader(f, delimiter=delim))
        except:
            return []

    customers = load_csv_safe("customers.csv")
    loans = load_csv_safe("loan_snapshots.csv")
    tasks = load_csv_safe("tasks.csv")
    participants = load_csv_safe("task_participants.csv")

    if not customers:
        return

    # 2. Mapping Data
    # PENTING: Mapping data CSV asli ke memory
    cust_map = {}
    for c in customers:
        key = c.get("customer_number") or c.get("id")
        if key:
            cust_map[key.strip()] = c

    cust_loans = {}
    for l in loans:
        key = l.get("customer_number")
        if key:
            cust_loans.setdefault(key.strip(), []).append(l)

    task_loc = {
        t["task_id"]: {"lat": t["latitude"], "lng": t["longitude"]}
        for t in tasks
        if "task_id" in t
    }
    loan_loc = {}
    for p in participants:
        pid = p.get("participant_id")
        tid = p.get("task_id")
        if pid and tid in task_loc:
            loan_loc[pid] = task_loc[tid]

    # 3. Generate Nodes
    all_cust_ids = list(cust_map.keys())
    processed_groups = {}

    images = (
        glob.glob(f"{IMAGE_DIR}/*.jpg")
        + glob.glob(f"{IMAGE_DIR}/*.jpeg")
        + glob.glob(f"{IMAGE_DIR}/*.png")
    )
    if not images:
        images = ["placeholder.jpg"]

    group_counter = 0

    for i in range(0, len(all_cust_ids), GROUP_SIZE):
        if group_counter >= MAX_NODES:
            break

        batch_ids = all_cust_ids[i : i + GROUP_SIZE]
        if not batch_ids:
            continue

        group_id = f"G{str(group_counter + 1).zfill(3)}"

        # --- A. Hitung Data CSV (Real) ---
        total_dpd = 0
        total_loan = 0
        businesses = []
        loc_candidates = []

        for cid in batch_ids:
            my_loans = cust_loans.get(cid, [])
            for l in my_loans:
                try:
                    total_dpd += int(float(l.get("dpd", 0)))
                except:
                    pass
                try:
                    total_loan += float(l.get("outstanding_amount", 0))
                except:
                    pass

                lid = l.get("loan_id")
                if lid in loan_loc:
                    loc_candidates.append(loan_loc[lid])

            if cid in cust_map:
                businesses.append(cust_map[cid].get("purpose", "Usaha Mikro"))

        avg_dpd = total_dpd / len(batch_ids) if batch_ids else 0
        common_biz = (
            max(set(businesses), key=businesses.count)
            if businesses
            else "Pedagang Umum"
        )

        # Logic Warna Node
        if avg_dpd > 30:
            risk_status = "TOXIC"
            node_color = "toxic"
        elif avg_dpd > 0:
            risk_status = "MEDIUM"
            node_color = "medium"
        else:
            risk_status = "HEALTHY"
            node_color = "healthy"

        # Logic Lokasi
        lat, lng = -6.59, 106.8
        if loc_candidates:
            try:
                lat = float(loc_candidates[0]["lat"])
                lng = float(loc_candidates[0]["lng"])
            except:
                pass
        else:
            lat += (random.random() - 0.5) * 0.05
            lng += (random.random() - 0.5) * 0.05

        # Image
        img_path = images[group_counter % len(images)]
        img_url = f"http://localhost:8000/static/{os.path.basename(img_path)}"

        # --- B. AI INTELLIGENCE (Hybrid) ---
        ai_data = {}

        # Cek apakah pakai AI atau Mockup
        if (
            group_counter < AI_LIMIT
            and GOOGLE_API_KEY != "MASUKKAN_API_KEY_ANDA_DISINI"
        ):
            # 🔴 REAL AI PATH
            try:
                print(f"   ✨ AI Processing {group_id}...")
                prompt = GROUP_ANALYSIS_PROMPT.format(
                    group_text=f"ID: {group_id}, DPD: {avg_dpd}, Biz: {common_biz}, Loan: {total_loan}"
                )
                inputs = [prompt]
                if img_path != "placeholder.jpg":
                    inputs.append(Image.open(img_path))

                resp = model.generate_content(
                    inputs, generation_config={"response_mime_type": "application/json"}
                )
                ai_data = json.loads(resp.text)
                time.sleep(1)
            except Exception as e:
                print(f"      ⚠️ AI Error: {e}")
                # Fallback ke mockup jika AI error

        if not ai_data:
            # 🔵 SMART MOCKUP PATH (Fallback Cerdas)
            # Kita generate data seolah-olah ini dari AI
            base_trust = (
                95
                if risk_status == "HEALTHY"
                else (70 if risk_status == "MEDIUM" else 40)
            )
            ai_data = {
                "risk_badge": f"{risk_status} RISK",
                "trust_score": base_trust,
                "sentiment_text": f"Kelompok didominasi usaha {common_biz}, performa pembayaran {risk_status.lower()}.",
                "asset_condition": "AVERAGE",
                "asset_tags": ["Usaha Mikro", "Bangunan Permanen"],
                "repayment_prediction": 98 if risk_status == "HEALTHY" else 60,
            }

        # --- C. CONSTRUCT JSON (FULL SCHEMA) ---
        # Ini bagian penting: Kita kembalikan semua field yang diminta FE

        trust_score = ai_data.get("trust_score", 70)

        processed_groups[group_id] = {
            "id": group_id,
            "type": node_color,
            "x": random.randint(0, 1000),
            "y": random.randint(0, 1000),
            "lat": lat,
            "lng": lng,
            "header": {
                "name": generate_group_name(group_counter + 1),
                "location_city": "Jawa Barat",
                "location_village": "Desa Binaan",
                "member_count": len(batch_ids),
                "risk_badge": ai_data.get("risk_badge"),
                "trust_score": trust_score,
                "loan_eligibility": "Eligible" if trust_score > 70 else "Review",
                "total_loan_amount": int(total_loan),
            },
            "overview": {
                "primary_driver": {
                    "text": ai_data.get("sentiment_text"),
                    "payment_score": ai_data.get("repayment_prediction"),
                    "social_score": trust_score,
                },
                "metrics": {
                    "cycle": random.randint(1, 10),
                    "repayment_rate": ai_data.get("repayment_prediction"),
                    "avg_delay": f"H+{int(avg_dpd)}",
                },
                "neighbors": [],  # Diisi nanti
            },
            # 🔥 FIELD LENGKAP UNTUK GRAFIK FE
            "trends": {
                "repayment_history": generate_trend_data(trust_score, is_asset=False),
                "asset_growth": generate_trend_data(
                    trust_score, is_asset=True
                ),  # Wajib ada
                "stats": {
                    "streak": random.randint(1, 12),
                    "last_default": "Never" if avg_dpd == 0 else "Active",
                    "trend_val": 2.5,
                    "trend_dir": "up" if trust_score > 70 else "down",
                    "avg_rate": 98.0,
                    "best_rate": 100.0,
                },
                "seasonality_heatmap": [
                    1,
                    1,
                    1,
                    2,
                    2,
                    3,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                ],  # Mock data heatmap
            },
            # 🔥 FIELD LENGKAP UNTUK INSIGHTS
            "insights": {
                "social_graph": {
                    "risk_members": generate_risk_members(
                        len(batch_ids), node_color
                    )  # Wajib ada untuk popup
                },
                "cv": {
                    "home": {
                        "condition": ai_data.get("asset_condition"),
                        "material": "Verified",
                        "roof": "Tile",
                        "access": "Paved",
                        "occupancy": "Occupied",
                        "assets": ai_data.get("asset_tags"),
                        "img_url": img_url,
                    },
                    "biz": {
                        "stability": "Permanent",
                        "type": common_biz,
                        "traffic": "Medium",
                        "status": "Active",
                        "digital": "QRIS",
                        "inventory": ["Full"],
                        "img_url": img_url,
                    },
                },
                "prediction": {  # Wajib ada
                    "default_risk_prob": 100 - trust_score,
                    "horizon_days": 30,
                    "what_if": {
                        "current_score": trust_score,
                        "projected_score": min(100, trust_score + 5),
                        "improvement_pct": 5,
                        "scenario": "Intervention",
                    },
                },
                "recommendation_text": f"Saran AI: Tingkatkan monitoring untuk kelompok {common_biz}.",
            },
            "decision": {
                "last_audit": f"Agent {random.choice(['Budi', 'Sari'])}",
                "is_locked": True if risk_status == "TOXIC" else False,
                "audit_date": datetime.now().strftime("%Y-%m-%d"),
            },
        }

        group_counter += 1
        print(f"✅ {group_id} Created | Trust: {trust_score}")

    # 4. NEIGHBORS WIRING
    gids = list(processed_groups.keys())
    for gid in processed_groups:
        neighbors = random.sample([x for x in gids if x != gid], k=3)
        processed_groups[gid]["overview"]["neighbors"] = []
        for nid in neighbors:
            n_data = processed_groups[nid]
            dist = random.randint(50, 500)
            rel = "Shared Agent"
            if dist < 100:
                rel = "Geo-Cluster"
            if n_data["type"] == "toxic":
                rel = "Risk Contagion"

            processed_groups[gid]["overview"]["neighbors"].append(
                {
                    "id": nid,
                    "name": n_data["header"]["name"],
                    "risk": n_data["type"],
                    "distance": f"{dist}m",
                    "relation": rel,
                }
            )

    # 5. SAVE FINAL JSON
    final_db = {
        "meta": {"version": "v-final-full-struct", "generated_at": str(datetime.now())},
        "global_state": {
            "wallet_balance": 1000000000,
            "spending_history": [100, 200, 300],
        },
        "groups": processed_groups,
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(final_db, f, indent=2)
    print(f"🎉 DONE! Database ({len(processed_groups)} nodes) saved to: {OUTPUT_JSON}")


if __name__ == "__main__":
    process_data()
