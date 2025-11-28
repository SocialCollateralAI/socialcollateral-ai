import csv
import glob
import json
import os
import random
import time
from datetime import datetime

# import google.generativeai as genai
from vertexai.preview.generative_models import GenerativeModel, Part
import vertexai

from PIL import Image

# ==========================================
# 🔧 KONFIGURASI PROJECT
# ==========================================
# Config from environment (override at deploy time)
GCP_PROJECT_ID = "valiant-student-479606-p6"
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "valiant-student-479606-p6")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")

vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "AIzaSyCW9TOk3TyICizVfATqx6qfJI35ztL75co")

# Path File
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "samples")
IMAGE_DIR = os.getenv("IMAGE_DIR", "data/images")
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "data/mock_db.json")
GCS_BUCKET = os.getenv("GCS_BUCKET")  # if set, upload output to this GCS bucket
AI_DELAY = float(os.getenv("AI_DELAY", "0.1"))

# --- SETTINGAN DEMO ---
GROUP_SIZE = 15  # 1 Kelompok = 15 Nasabah
MAX_NODES = 10  # Total Node yang dibuat
AI_LIMIT = 1000  # 20 Node pertama pakai Real AI, sisanya Smart Mockup

# --- VISUAL SIZING SYSTEM ---
# 🎯 SISTEM UKURAN NODE BERDASARKAN URGENSI PERHATIAN:
# • HEALTHY (Hijau): 25-50px = Trust Score TINGGI → Node BESAR → Prioritas Modal Tinggi ✅💰
# • MEDIUM (Kuning): 15-25px = Perlu Review Lebih 🔍⚠️  
# • TOXIC (Merah): 15-40px = Trust Score RENDAH → Node BESAR → Urgensi Penanganan Tinggi! ❌🚨
# 
# LOGIKA VISUAL:
# - HEALTHY: Makin bagus performance → Makin besar → Prioritas modal
# - TOXIC: Makin buruk performance → Makin besar → Butuh perhatian segera!

# Setup Gemini
# if not GOOGLE_API_KEY:
#     print("⚠️  PERINGATAN: API Key belum diisi. AI calls will be skipped.")
# else:
#     genai.configure(api_key=GOOGLE_API_KEY)

model_names = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-image",
]
model = None
for mn in model_names:
    try:
        # Try to initialize model; don't run a test generate here in containerized env
        candidate = GenerativeModel(mn)
        model = candidate
        print(f"✅ Google AI model initialized: {mn}")
        break
    except Exception as e:
        print(f"⚠️ Model {mn} not available or unsupported: {e}")

if model is None:
    print("⚠️ No Gemini model initialized. AI calls will be skipped or will fall back.")
    AI_AVAILABLE = False
else:
    AI_AVAILABLE = True

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


def generate_random_location():
    """Generate random Jabodetabek location"""
    jabodetabek_cities = [
        "Jakarta Pusat", "Jakarta Utara", "Jakarta Selatan", "Jakarta Timur", "Jakarta Barat",
        "Bogor", "Depok", "Tangerang", "Bekasi", "Tangerang Selatan"
    ]
    
    villages = [
        "Desa Maju Jaya", "Desa Sejahtera", "Desa Makmur", "Desa Sentosa", "Desa Barokah",
        "Desa Sinar Harapan", "Desa Cahaya Baru", "Desa Mandiri", "Desa Bersama", "Desa Lestari",
        "Kelurahan Merdeka", "Kelurahan Bina Karya", "Kelurahan Sukamaju", "Kelurahan Harmoni",
        "Kampung Damai", "Kampung Rukun", "Kampung Gotong Royong", "Kampung Makmur Jaya"
    ]
    
    return {
        "city": random.choice(jabodetabek_cities),
        "village": random.choice(villages)
    }


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


def generate_modal_recommendation(trust_score, risk_status, business_type, node_size):
    """🎯 SMART MODAL RECOMMENDATION berdasarkan visual size dan risk profile"""
    
    if risk_status == "HEALTHY" and node_size >= 40:
        return f"🟢 PRIORITAS TINGGI: Kelompok {business_type} sangat layak modal besar. Trust score {trust_score}% excellent!"
    elif risk_status == "HEALTHY" and node_size >= 30:
        return f"🟢 LAYAK MODAL: Kelompok {business_type} dapat diberikan modal standar. Performance bagus."
    elif risk_status == "HEALTHY":
        return f"🟢 MODAL KECIL: Kelompok {business_type} layak modal terbatas. Trust score {trust_score}% cukup baik."
    elif risk_status == "MEDIUM" and node_size >= 20:
        return f"🟡 REVIEW DETAIL: Kelompok {business_type} perlu evaluasi mendalam sebelum pemberian modal."
    elif risk_status == "MEDIUM":
        return f"🟡 MODAL MIKRO: Kelompok {business_type} hanya layak modal sangat terbatas dengan monitoring ketat."
    elif node_size >= 30:  # TOXIC dengan size besar = urgensi tinggi
        return f"🔴 URGENSI TINGGI: Kelompok {business_type} trust score {trust_score}% - Butuh intervensi segera! Tidak layak modal."
    else:  # TOXIC dengan size kecil
        return f"🔴 TIDAK LAYAK: Kelompok {business_type} tidak direkomendasikan untuk modal. Fokus recovery dulu."


# ==========================================
# 🚀 DATA PROCESSOR
# ==========================================
def process_data():
    global AI_AVAILABLE
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

    # Separate home and business images
    home_images = (
        glob.glob(f"{IMAGE_DIR}/home/*.jpg")
        + glob.glob(f"{IMAGE_DIR}/home/*.jpeg")
        + glob.glob(f"{IMAGE_DIR}/home/*.png")
    )
    bisnis_images = (
        glob.glob(f"{IMAGE_DIR}/bisnis/*.jpg")
        + glob.glob(f"{IMAGE_DIR}/bisnis/*.jpeg")
        + glob.glob(f"{IMAGE_DIR}/bisnis/*.png")
    )
    
    if not home_images:
        home_images = ["placeholder_home.jpg"]
    if not bisnis_images:
        bisnis_images = ["placeholder_bisnis.jpg"]

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

        # Separate images for home and business
        home_img_path = home_images[group_counter % len(home_images)]
        bisnis_img_path = bisnis_images[group_counter % len(bisnis_images)]
        
        # Generate URLs - check if files actually exist
        if "placeholder" not in home_img_path and os.path.exists(home_img_path):
            home_img_url = f"data/images/home/{os.path.basename(home_img_path)}"
        else:
            home_img_url = "placeholder_home.jpg"
            
        if "placeholder" not in bisnis_img_path and os.path.exists(bisnis_img_path):
            bisnis_img_url = f"data/images/bisnis/{os.path.basename(bisnis_img_path)}"
        else:
            bisnis_img_url = "placeholder_bisnis.jpg"

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
                # Use home image for AI analysis (as it's more relevant for risk assessment)
                if "placeholder" not in home_img_path and os.path.exists(home_img_path):
                    try:
                        inputs.append(Image.open(home_img_path))
                    except Exception as img_err:
                        print(f"      ⚠️ Legacy image processing error: {img_err}")
                        # Continue without image

                # resp = model.generate_content(
                #     inputs, generation_config={"response_mime_type": "application/json"}
                # )
                # ai_data = json.loads(resp.text)
                vertex_inputs = []

                # ➕ Add Text Prompt
                vertex_inputs.append(prompt)

                # ➕ Add Image (if exists) - use home image for risk analysis
                if "placeholder" not in home_img_path and os.path.exists(home_img_path):
                    try:
                        # Determine MIME type based on file extension
                        ext = os.path.splitext(home_img_path)[1].lower()
                        mime_type = "image/jpeg"
                        if ext in ['.png']:
                            mime_type = "image/png"
                        elif ext in ['.jpg', '.jpeg']:
                            mime_type = "image/jpeg"
                        
                        # Read image as bytes for Vertex AI
                        with open(home_img_path, 'rb') as img_file:
                            image_data = img_file.read()
                        vertex_inputs.append(
                            Part.from_data(data=image_data, mime_type=mime_type)
                        )
                    except Exception as img_err:
                        print(f"      ⚠️ Image processing error: {img_err}")
                        # Continue without image

                # 🔥 GENERATE WITH VERTEX
                resp = model.generate_content(
                    vertex_inputs,
                    generation_config={
                        "max_output_tokens": 2048,
                        "temperature": 0.3,
                        "response_mime_type": "application/json"
                    }
                )

                ai_data = json.loads(resp.text)
                time.sleep(AI_DELAY)
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

        # 🎯 VISUAL SIZE LOGIC - Size menunjukkan urgensi perhatian
        # HEALTHY: Semakin tinggi trust score = Semakin besar (prioritas modal tinggi)
        # TOXIC: Semakin rendah trust score = Semakin besar (urgensi penanganan tinggi)
        def calculate_node_size(trust_score, risk_status):
            base_size = 20
            
            if risk_status == "HEALTHY":
                # Healthy: 25-50 (Semakin tinggi trust score = Semakin besar = Prioritas modal tinggi)
                size = base_size + int((trust_score - 70) * 0.7) + 5
                return max(25, min(50, size))
            elif risk_status == "MEDIUM":
                # Medium: 15-25 (Sedang-sedang saja)
                size = base_size + int((trust_score - 50) * 0.25)
                return max(15, min(25, size))
            else:  # TOXIC
                # Toxic: 15-40 (Semakin RENDAH trust score = Semakin BESAR = Urgensi penanganan tinggi!)
                # Inversi: trust_score rendah = size besar
                inverted_score = 100 - trust_score  # Flip the score
                size = 15 + int(inverted_score * 0.4)  # Semakin rendah trust, semakin besar node
                return max(15, min(40, size))

        node_size = calculate_node_size(trust_score, risk_status)

        # Generate random location (moved outside dict to avoid syntax error)
        location = generate_random_location()

        processed_groups[group_id] = {
            "id": group_id,
            "type": node_color,
            "size": node_size,  # 🔥 NEW: Visual indicator untuk kelayakan modal
            "x": random.randint(0, 1000),
            "y": random.randint(0, 1000),
            "lat": lat,
            "lng": lng,
            "header": {
                "name": generate_group_name(group_counter + 1),
                "location_city": location["city"],
                "location_village": location["village"],
                "member_count": len(batch_ids),
                "risk_badge": ai_data.get("risk_badge"),
                "trust_score": trust_score,
                "loan_eligibility": "Eligible" if trust_score > 70 else "Review",
                "total_loan_amount": int(total_loan),
                "visual_priority": "HIGH" if (risk_status == "HEALTHY" and node_size >= 35) or (risk_status == "TOXIC" and node_size >= 30) else "MEDIUM" if node_size >= 20 else "LOW",  # 🔥 NEW: Visual cue untuk frontend
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
                        "img_url": home_img_url,
                    },
                    "biz": {
                        "stability": "Permanent",
                        "type": common_biz,
                        "traffic": "Medium",
                        "status": "Active",
                        "digital": "QRIS",
                        "inventory": ["Full"],
                        "img_url": bisnis_img_url,
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
                "recommendation_text": generate_modal_recommendation(trust_score, risk_status, common_biz, node_size),
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

    # Upload to GCS if requested
    if GCS_BUCKET:
        try:
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob_name = os.path.basename(OUTPUT_JSON)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(json.dumps(final_db, indent=2), content_type="application/json")
            print(f"☁️ Uploaded result to: gs://{GCS_BUCKET}/{blob_name}")
        except Exception as e:
            print(f"⚠️ Failed uploading to GCS: {e}")


if __name__ == "__main__":
    process_data()
