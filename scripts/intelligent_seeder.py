import json
import os
import random
from datetime import datetime

from faker import Faker

fake = Faker("id_ID")

TOTAL_NODES = 100
RATIO_TOXIC = 0.20  # 20% Toxic
OUTPUT_FILE = "data/mock_db.json"

# ASSET BANK (SAMPLE/REAL IMAGES)
# Unsplash Source API
CV_ASSETS = {
    "healthy": {
        "home": [
            "https://images.unsplash.com/photo-1605276374104-dee2a0ed3cd6?auto=format&fit=crop&q=80&w=300&h=200",  # Rumah layak
            "https://images.unsplash.com/photo-1568605114967-8130f3a36994?auto=format&fit=crop&q=80&w=300&h=200",
            "https://images.unsplash.com/photo-1583608205776-bfd35f0d9f83?auto=format&fit=crop&q=80&w=300&h=200",
        ],
        "biz": [
            "https://images.unsplash.com/photo-1534723452862-4c874018d66d?auto=format&fit=crop&q=80&w=300&h=200",  # Toko penuh/ramai
            "https://images.unsplash.com/photo-1604719312566-b7cb04a461bc?auto=format&fit=crop&q=80&w=300&h=200",
            "https://images.unsplash.com/photo-1556740758-90de2929e79a?auto=format&fit=crop&q=80&w=300&h=200",
        ],
    },
    "toxic": {
        "home": [
            "https://images.unsplash.com/photo-1518780664697-55e3ad937233?auto=format&fit=crop&q=80&w=300&h=200",  # Rumah kurang layak
            "https://images.unsplash.com/photo-1480074568708-e7b720bb3f09?auto=format&fit=crop&q=80&w=300&h=200",
        ],
        "biz": [
            "https://images.unsplash.com/photo-1555396273-367ea4eb4db5?auto=format&fit=crop&q=80&w=300&h=200",  # Toko tutup/sepi
            "https://images.unsplash.com/photo-1562564055-71e051d33c19?auto=format&fit=crop&q=80&w=300&h=200",
        ],
    },
}

# NARRATIVE BANK (DYNAMIC SNIPPETS)
HEALTHY_NARRATIVES = [
    "Ketua kelompok sangat aktif mengorganisir arisan mingguan.",
    "Anggota saling menalangi jika ada yang telat bayar, solidaritas tinggi.",
    "Usaha warung sembako anggota berkembang pesat, stok selalu habis.",
    "Pembayaran lancar, tidak ada catatan tunggakan dalam 6 bulan terakhir.",
    "Hubungan antar anggota sangat harmonis, sering mengadakan pengajian rutin.",
    "Kondisi ekonomi desa stabil karena panen raya berhasil.",
    "Agen melaporkan bahwa ibu-ibu sangat disiplin hadir tepat waktu.",
]

TOXIC_NARRATIVES = [
    "Ditemukan indikasi konflik internal antara ketua dan bendahara.",
    "Tiga anggota sulit ditemui saat penagihan rutin.",
    "Usaha ternak lele gagal total karena banjir, memicu gagal bayar.",
    "Ada laporan anggota menggunakan uang pinjaman untuk konsumtif (beli motor).",
    "Ketua kelompok pindah domisili tanpa pemberitahuan jelas.",
    "Terjadi pertengkaran saat pertemuan mingguan terkait uang kas.",
    "Agen kesulitan menghubungi anggota, nomor HP tidak aktif.",
]

MEDIUM_NARRATIVES = [
    "Pembayaran sedikit tersendat karena menunggu masa panen.",
    "Ada satu anggota sakit, namun anggota lain siap membantu.",
    "Usaha musiman sedang sepi, arus kas sedikit terganggu.",
    "Komunikasi dengan agen cukup baik meski pembayaran tidak selalu tepat waktu.",
]

# RELATION TYPES
RELATION_TYPES = [
    "High Co-Repayment Pattern",
    "Shared Field Agent (Pak Budi)",
    "Geo-Cluster (< 50m)",
    "Historical Guarantor",
]


def generate_smart_data(gid):
    # Tentukan Nasib
    rand = random.random()
    if rand < RATIO_TOXIC:
        risk_type = "toxic"
    elif rand < RATIO_TOXIC + 0.3:
        risk_type = "medium"
    else:
        risk_type = "healthy"

    # "HERO & VILLAIN" HARDCODING
    if gid == "G001":
        risk_type = "healthy"  # HERO
    if gid == "G004":
        risk_type = "toxic"  # VILLAIN

    # Logic Data
    if risk_type == "healthy":
        trust_score = random.randint(85, 99)
        risk_badge = "LOW RISK"
        loan_eligibility = "Eligible"
        repayment_rate = round(random.uniform(96.0, 100.0), 1)
        avg_delay = "H+0"
        streak = random.randint(5, 12)

        # Narrative Injection
        snippets = random.sample(HEALTHY_NARRATIVES, k=2)
        nlp_snippet = f"Laporan Agen: '{snippets[0]} {snippets[1]}'"
        sentiment_score = 85

        # Real Asset Injection
        cv_img_home = random.choice(CV_ASSETS["healthy"]["home"])
        cv_img_biz = random.choice(CV_ASSETS["healthy"]["biz"])
        cv_condition = "GOOD"

        heatmap = [1] * 12

    elif risk_type == "toxic":
        trust_score = random.randint(10, 40)
        risk_badge = "HIGH RISK"
        loan_eligibility = "Restricted"
        repayment_rate = round(random.uniform(20.0, 60.0), 1)
        avg_delay = f"H+{random.randint(7, 30)}"
        streak = 0

        snippets = random.sample(TOXIC_NARRATIVES, k=2)
        nlp_snippet = f"Laporan Agen: '{snippets[0]} {snippets[1]}'"
        sentiment_score = 25

        cv_img_home = random.choice(CV_ASSETS["toxic"]["home"])
        cv_img_biz = random.choice(CV_ASSETS["toxic"]["biz"])
        cv_condition = "POOR"

        heatmap = [1, 1, 2, 3, 3, 3, 2, 1, 1, 2, 3, 3]

    else:  # Medium
        trust_score = random.randint(50, 80)
        risk_badge = "MED RISK"
        loan_eligibility = "Review"
        repayment_rate = round(random.uniform(75.0, 95.0), 1)
        avg_delay = f"H+{random.randint(1, 7)}"
        streak = random.randint(0, 3)

        snippets = random.sample(MEDIUM_NARRATIVES, k=1)
        nlp_snippet = f"Laporan Agen: '{snippets[0]}'"
        sentiment_score = 55

        cv_img_home = random.choice(CV_ASSETS["healthy"]["home"])  # Pake yg bagus aja
        cv_img_biz = random.choice(CV_ASSETS["toxic"]["biz"])  # Tapi usaha sepi
        cv_condition = "AVERAGE"
        heatmap = [random.randint(1, 2) for _ in range(12)]

    # HERO SPECIFIC OVERRIDES
    if gid == "G001":
        nlp_snippet = "Laporan Agen: 'Ibu Sari sangat inspiratif, menalangi 2 anggota yang sakit DBD minggu lalu. Solidaritas level tertinggi di desa ini.'"
        trust_score = 98

    if gid == "G004":
        nlp_snippet = "Laporan Agen: 'Terjadi adu mulut hebat saat penagihan. Bendahara diduga menggunakan uang kas untuk judi online. Sangat berisiko.'"
        trust_score = 12

    # Construct JSON Object
    return {
        "id": gid,
        "type": risk_type,
        "x": random.randint(0, 1000),
        "y": random.randint(0, 1000),
        "header": {
            "name": f"KELOMPOK {fake.first_name().upper()}",
            "location_city": "Bogor",
            "location_village": random.choice(["Desa Ciseeng", "Desa Parung"]),
            "member_count": random.randint(5, 25),
            "risk_badge": risk_badge,
            "trust_score": trust_score,
            "loan_eligibility": loan_eligibility,
            "total_loan_amount": random.randint(20, 200) * 1000000,
        },
        "overview": {
            "primary_driver": {
                "text": nlp_snippet,
                "payment_score": int(repayment_rate),
                "social_score": sentiment_score,
            },
            "metrics": {
                "cycle": random.randint(1, 8),
                "repayment_rate": repayment_rate,
                "avg_delay": avg_delay,
            },
            "neighbors": [],
            "max_plafon_recommendation": 0 if risk_type == "toxic" else 50000000,
        },
        "trends": {
            "repayment_history": [
                {
                    "month": m,
                    "rate": max(
                        0, min(100, int(repayment_rate + random.uniform(-5, 5)))
                    ),
                }
                for m in ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun"]
            ],
            "asset_growth": [
                {"month": m, "value": random.randint(20, 60)}
                for m in ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun"]
            ],
            "stats": {
                "streak": streak,
                "last_default": "Feb 2023" if risk_type == "toxic" else "Tidak Pernah",
                "trend_val": 2.5,
                "trend_dir": "up",
                "avg_rate": repayment_rate,
                "best_rate": repayment_rate + 5,
            },
            "seasonality_heatmap": heatmap,
        },
        "insights": {
            "social_graph": {
                "risk_members": [
                    {"name": fake.name(), "risk_score": "30%", "hops": "1 hop"}
                ]
            },
            "cv": {
                "home": {
                    "condition": cv_condition,
                    "material": "Cement",
                    "roof": "Tile",
                    "access": "Paved",
                    "occupancy": "Occupied",
                    "assets": ["TV", "Motor"],
                    "img_url": cv_img_home,  # REAL URL
                },
                "biz": {
                    "stability": "Permanent",
                    "type": "Warung",
                    "traffic": "High",
                    "status": "Active",
                    "digital": "QRIS",
                    "inventory": ["Full"],
                    "img_url": cv_img_biz,  # REAL URL
                },
            },
            "prediction": {
                "default_risk_prob": 100 - trust_score,
                "horizon_days": 30,
                "what_if": {
                    "current_score": trust_score,
                    "projected_score": trust_score + 10,
                    "improvement_pct": 10,
                    "scenario": "Intervention",
                },
            },
            "recommendation_text": "Immediate intervention required."
            if risk_type == "toxic"
            else "Maintain monitoring.",
        },
        "decision": {
            "last_audit": f"Terakhir diaudit oleh {fake.name()} (2 jam lalu)",
            "is_locked": True if risk_type == "toxic" else False,
        },
    }


def main():
    print("Generating Intelligent Data...")
    groups = {}

    for i in range(TOTAL_NODES):
        # Paksa ID G001 dan G004 ada
        gid = f"G{str(i + 1).zfill(3)}"
        groups[gid] = generate_smart_data(gid)

    # Generate Edges dengan Label PROPOSAL
    gids = list(groups.keys())
    for gid in groups:
        neighbors = random.sample(gids, k=random.randint(2, 3))
        neighbor_data = []
        for nid in neighbors:
            if nid == gid:
                continue
            n_obj = groups[nid]

            # LOGIKA RELASI
            rel_type = random.choice(RELATION_TYPES)
            if n_obj["type"] == "toxic":
                rel_type = "Risk Contagion (NPL Link)"  # Khusus toxic

            neighbor_data.append(
                {
                    "name": n_obj["header"]["name"],
                    "risk": n_obj["type"],
                    "distance": f"{random.randint(10, 500)}m",
                    "relation": rel_type,  # <-- VALIDASI EDGES DISINI
                }
            )
        groups[gid]["overview"]["neighbors"] = neighbor_data

    # Save
    final_db = {
        "meta": {"version": "v4-final", "total": TOTAL_NODES},
        "global_state": {
            "wallet_balance": 1000000000000,
            "spending_history": [1000, 990, 980],
        },
        "groups": groups,
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_db, f, indent=2)
    print(f"Data Generated to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
