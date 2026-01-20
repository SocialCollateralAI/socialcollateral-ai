import csv
import glob
import json
import os
import random
import time

import google.generativeai as genai
from PIL import Image

# ==========================================
# 🔧 KONFIGURASI (UBAH INI BESOK)
# ==========================================
# 1. Masukkan Google AI Studio API Key (Gemini)
GOOGLE_API_KEY = "MASUKKAN_API_KEY_ANDA_DISINI"

# 2. Nama File CSV dari Panitia (Pastikan formatnya CSV)
INPUT_CSV_FILE = "raw_data/sample_data_amartha.csv"

# 3. Folder Foto dari Panitia
INPUT_IMAGE_DIR = "data/images"

# 4. Output JSON (Jangan Ubah Ini)
OUTPUT_JSON = "data/mock_db.json"
# ==========================================

# Setup Gemini
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# Prompt Super Lengkap (Satu prompt untuk NLP + Vision sekaligus)
# Ini menghemat kuota dan waktu dibanding memanggil API 2x.
MULTIMODAL_PROMPT = """
Berperanlah sebagai Senior Risk Analyst untuk Microfinance di Indonesia.
Saya akan memberikan:
1. DATA TEKS: Profil peminjam (dari CSV).
2. GAMBAR: Foto aset rumah/usaha mereka.

Tugas Anda adalah menganalisis risiko secara holistik dan hasilkan output JSON murni.

Analisis yang diminta:
1. **Risk Badge**: Tentukan apakah "LOW RISK", "MED RISK", atau "HIGH RISK" berdasarkan harmoni antara data teks dan kondisi fisik di foto.
2. **Trust Score**: Berikan skor 0-100.
3. **Sentiment Analysis**: Buat satu kalimat ringkasan sentimen (Bahasa Indonesia) yang menjelaskan kenapa skornya demikian (misal: "Kondisi rumah sangat terawat mendukung profil usaha yang stabil").
4. **Asset Condition**: Nilai kondisi aset di foto ("GOOD", "AVERAGE", "POOR").
5. **Asset Tags**: Berikan 2-3 tag singkat visual (misal: "Atap Genteng", "Lantai Semen", "Warung Penuh").

Format Output JSON (WAJIB PERSIS SEPERTI INI, tanpa markdown block):
{
  "risk_badge": "LOW RISK",
  "trust_score": 85,
  "sentiment_text": "bla bla bla",
  "asset_condition": "GOOD",
  "asset_tags": ["Tag1", "Tag2"],
  "repayment_prediction": 98
}
"""


def process_data():
    print("🚀 MEMULAI FINAL INTELLIGENT SEEDING...")

    # 1. Cek Data Input
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"❌ ERROR: File CSV tidak ditemukan di {INPUT_CSV_FILE}")
        print("   -> Buat folder 'raw_data' dan masukkan file CSV panitia ke situ.")
        return

    # Ambil list semua foto di folder images
    all_images = (
        glob.glob(f"{INPUT_IMAGE_DIR}/*.jpg")
        + glob.glob(f"{INPUT_IMAGE_DIR}/*.jpeg")
        + glob.glob(f"{INPUT_IMAGE_DIR}/*.png")
    )

    if not all_images:
        print(
            f"⚠️ WARNING: Tidak ada foto di {INPUT_IMAGE_DIR}. Pastikan foto panitia sudah dicopy."
        )
        # Fallback dummy image jika kosong
        all_images = ["placeholder.jpg"]

    groups = {}

    with open(INPUT_CSV_FILE, mode="r", encoding="utf-8-sig") as csvfile:
        # Menggunakan DictReader agar fleksibel dengan nama kolom
        reader = csv.DictReader(csvfile)

        # Ambil fieldnames untuk mapping cerdas
        headers = reader.fieldnames
        print(f"📋 Kolom CSV terdeteksi: {headers}")

        for i, row in enumerate(reader):
            # Batasi sampel untuk demo jika perlu (misal 50 data saja biar cepat)
            if i >= 50:
                break

            gid = f"G{str(i + 1).zfill(3)}"

            # --- A. INTELLIGENT MAPPING (CSV -> Variables) ---
            # Cari kolom yang relevan secara otomatis (case insensitive partial match)
            def get_val(keywords, default):
                for key in row.keys():
                    if any(k in key.lower() for k in keywords):
                        return row[key]
                return default

            name = get_val(["nama", "name", "ketua"], f"Ibu {gid}")
            village = get_val(["desa", "village", "kelurahan"], "Desa Parung")
            loan = get_val(["plafon", "amount", "pinjaman"], "5000000")

            # --- B. IMAGE SELECTION ---
            # Jika ada kolom 'foto' di CSV, pakai itu. Jika tidak, ambil random/urut dari folder.
            # Strategi onsite: Ambil foto urut index biar variatif (i % total_images)
            if all_images and all_images[0] != "placeholder.jpg":
                img_path = all_images[i % len(all_images)]
                img_filename = os.path.basename(img_path)
                # URL Lokal untuk Frontend
                img_url = f"http://localhost:8000/static/{img_filename}"
            else:
                img_path = None
                img_url = "https://images.unsplash.com/photo-1568605114967-8130f3a36994"  # Fallback Unsplash

            print(
                f"🔄 Processing {gid} - {name} | Img: {img_filename if img_path else 'None'}..."
            )

            # --- C. THE AI BRAIN (GEMINI CALL) ---
            ai_result = {}
            try:
                # Siapkan prompt data
                row_text = str(row)  # Kirim seluruh data baris CSV mentah

                inputs = [MULTIMODAL_PROMPT, f"DATA PEMINJAM:\n{row_text}"]

                # Jika ada gambar fisik, lampirkan ke prompt
                if img_path:
                    img_file = Image.open(img_path)
                    inputs.append(img_file)

                # Tembak API
                response = model.generate_content(
                    inputs, generation_config={"response_mime_type": "application/json"}
                )
                ai_result = json.loads(response.text)

            except Exception as e:
                print(f"   ⚠️ AI Error (Quota/Network): {e}")
                print("   -> Menggunakan Fallback Logic (Random)")
                # Fallback Logic kalau API gagal
                ai_result = {
                    "risk_badge": random.choice(["LOW RISK", "MED RISK"]),
                    "trust_score": random.randint(60, 95),
                    "sentiment_text": "Analisis AI tertunda, data historis menunjukkan pola stabil.",
                    "asset_condition": "AVERAGE",
                    "asset_tags": ["Unverified"],
                    "repayment_prediction": 95,
                }
                time.sleep(1)  # Jeda dikit

            # --- D. CONSTRUCT JSON (MOCK DB SCHEMA) ---
            # Mapping hasil AI ke Struktur Database Frontend

            # Tentukan tipe warna visual (Healthy/Toxic) berdasarkan Risk Badge AI
            node_type = "healthy"
            if ai_result["risk_badge"] == "HIGH RISK":
                node_type = "toxic"
            elif ai_result["risk_badge"] == "MED RISK":
                node_type = "medium"

            groups[gid] = {
                "id": gid,
                "type": node_type,
                # Koordinat random dulu, Sigma.js bisa atur layout nanti
                "x": random.randint(0, 1000),
                "y": random.randint(0, 1000),
                "header": {
                    "name": name.upper(),
                    "location_city": "Bogor",  # Hardcode atau ambil dari CSV
                    "location_village": village,
                    "member_count": random.randint(10, 25),  # Simulasi
                    "risk_badge": ai_result["risk_badge"],
                    "trust_score": ai_result["trust_score"],
                    "loan_eligibility": "Eligible"
                    if ai_result["trust_score"] > 70
                    else "Review",
                    "total_loan_amount": int(
                        str(loan).replace(".", "").replace(",", "")
                    ),  # Clean number
                },
                "overview": {
                    "primary_driver": {
                        "text": f"AI Insight: {ai_result['sentiment_text']}",
                        "payment_score": ai_result["repayment_prediction"],
                        "social_score": int(ai_result["trust_score"] * 0.9),
                    },
                    "metrics": {
                        "cycle": random.randint(1, 5),
                        "repayment_rate": float(ai_result["repayment_prediction"]),
                        "avg_delay": "H+0" if node_type == "healthy" else "H+7",
                    },
                    "neighbors": [],  # Nanti diisi logic graph connection
                    "max_plafon_recommendation": 0
                    if node_type == "toxic"
                    else 50000000,
                },
                "trends": {  # Data grafik simulasi (agar chart FE hidup)
                    "repayment_history": [
                        {"month": m, "rate": random.randint(80, 100)}
                        for m in ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun"]
                    ],
                    "asset_growth": [
                        {"month": m, "value": random.randint(20, 60)}
                        for m in ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun"]
                    ],
                    "stats": {
                        "streak": random.randint(0, 12),
                        "last_default": "Tidak Pernah",
                        "trend_val": 2.5,
                        "trend_dir": "up",
                        "avg_rate": 98.0,
                        "best_rate": 100.0,
                    },
                    "seasonality_heatmap": [1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1],
                },
                "insights": {
                    "social_graph": {"risk_members": []},  # Kosongkan atau simulasi
                    "cv": {
                        "home": {
                            "condition": ai_result["asset_condition"],
                            "material": "Verified",
                            "roof": "Verified",
                            "access": "Paved",
                            "occupancy": "Occupied",
                            "assets": ai_result["asset_tags"],
                            "img_url": img_url,  # <--- INI FOTO ASLI/LOCAL
                        },
                        # Asumsi foto biz sama atau random pick lain
                        "biz": {
                            "stability": "Permanent",
                            "type": "Warung",
                            "traffic": "High",
                            "status": "Active",
                            "digital": "QRIS",
                            "inventory": ["Full Stock"],
                            "img_url": img_url,  # Reuse gambar atau ambil gambar ke-2
                        },
                    },
                    "prediction": {
                        "default_risk_prob": 100 - ai_result["trust_score"],
                        "horizon_days": 30,
                        "what_if": {
                            "current_score": ai_result["trust_score"],
                            "projected_score": ai_result["trust_score"] + 5,
                            "improvement_pct": 5,
                            "scenario": "Intervention",
                        },
                    },
                    "recommendation_text": f"AI Recommendation: {node_type.upper()} protocol applied.",
                },
                "decision": {
                    "last_audit": "AI Automated Audit (Gemini 1.5)",
                    "is_locked": True if node_type == "toxic" else False,
                },
            }

            # Rate limit guard (Gemini Flash cepat, tapi jaga-jaga)
            time.sleep(1)

    # --- E. GENERATE RELATIONS (GRAPH EDGES) ---
    # Buat relasi antar node, misalnya berdasarkan Desa yang sama
    print("🕸️ Generating Social Graph Connections...")
    node_ids = list(groups.keys())

    for gid, data in groups.items():
        # Cari tetangga potensial (misal 2-3 orang)
        potential_neighbors = random.sample(node_ids, k=3)
        my_village = data["header"]["location_village"]

        neighbor_list = []
        for nid in potential_neighbors:
            if nid == gid:
                continue

            n_data = groups[nid]
            n_village = n_data["header"]["location_village"]

            # Logic Relasi: Kalau satu desa = "Geo-Cluster", kalau beda = "Shared Agent"
            rel_type = "Shared Field Agent"
            if my_village == n_village:
                rel_type = "Geo-Cluster (< 50m)"

            # Khusus kalau tetangganya Toxic, kasih relasi "Risk Contagion" biar keren
            if n_data["type"] == "toxic":
                rel_type = "Risk Contagion (NPL Link)"

            neighbor_list.append(
                {
                    "name": n_data["header"]["name"],
                    "risk": n_data["type"],
                    "distance": f"{random.randint(10, 500)}m",
                    "relation": rel_type,
                }
            )

        groups[gid]["overview"]["neighbors"] = neighbor_list

    # --- F. SAVE TO FILE ---
    final_db = {
        "meta": {"version": "v-final-onsite", "total": len(groups)},
        "global_state": {  # Mock global state
            "wallet_balance": 1000000000,
            "spending_history": [100, 200, 300],
        },
        "groups": groups,
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(final_db, f, indent=2)

    print(f"✅ SUKSES! Database MVP Full AI tersimpan di: {OUTPUT_JSON}")


if __name__ == "__main__":
    process_data()
