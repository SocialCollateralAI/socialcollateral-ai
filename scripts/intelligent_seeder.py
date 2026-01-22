import csv
import glob
import json
import os
import random
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import io

# import google.generativeai as genai
from vertexai.preview.generative_models import GenerativeModel, Part
import vertexai

from PIL import Image

# ==========================================
# GCP PROJECT CONFIGURATION
# ==========================================
# NOTE: This configuration represents the setup used during the
# Amartha x GDG Jakarta 2025 Hackathon. GCP credits have been exhausted
# and API keys are no longer active. This code is preserved for
# documentation and portfolio purposes.
#
# To run this seeder with live GCP inference, you would need:
# - Active GCP project with Vertex AI enabled
# - Valid API credentials
# - Sufficient quota allocation

# Historical GCP Project ID (no longer active)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "valiant-student-479606-p6")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")

vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)

# Historical Google API Key (expired - for documentation reference only)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "AIzaSyCW9TOk3TyICizVfATqx6qfJI35ztL75co")

# File paths
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "samples")
IMAGE_DIR = os.getenv("IMAGE_DIR", "data/images")
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "data/mock_db.json")
GCS_BUCKET = os.getenv("GCS_BUCKET")  # Optional: upload output to GCS bucket

# Demo configuration
GROUP_SIZE = 15  # Members per group (Amartha group lending model)
MAX_NODES = 100  # Total Node yang dibuat
AI_LIMIT = 1000
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))  # Parallel processing threads (reduced for API quota)


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

# AI prompt for Gemini model
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
  "repayment_prediction": (Integer 0-100),
  "recommendation_text": "Teks rekomendasi modal (misal: 'Prioritas Tinggi: Kelompok sangat layak modal besar.')"
}}
"""

# Data generator functions


def generate_group_name(index):
    """Nama Kelompok Realistis"""
    prefix = ["Kelompok", "Paguyuban", "Koperasi", "Mitra"]
    adjective = [
        "Maju",
        "Sejahtera",
        "Makmur",
        "Sentosa",
        "Barokah",
    ]
    noun = [
        "Jaya",
        "Abadi",
        "Lestari",
        "Berkah",
        "Usaha",
    ]
    random.seed(index)
    name = f"{random.choice(prefix)} {random.choice(adjective)} {random.choice(noun)} {index}"
    random.seed(time.time())
    return name.upper()


def generate_random_location():
    """Generate random Jabodetabek location"""
    jabodetabek_cities = [
        "Jakarta Pusat", "Jakarta Utara", "Jakarta Selatan", "Jakarta Timur", "Jakarta Barat"
    ]
    
    villages = [
        "Desa Maju Jaya", "Desa Sejahtera", "Desa Makmur", "Desa Sentosa", "Desa Barokah",
        "Desa Sinar Harapan", "Desa Cahaya Baru", "Desa Mandiri", "Desa Bersama",
    ]
    
    return {
        "city": random.choice(jabodetabek_cities),
        "village": random.choice(villages)
    }


def generate_trend_data(trust_score, is_asset=False):
    """
    SMART MOCK: Membuat array grafik yang masuk akal sesuai skor.
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
    """MOCK: Daftar anggota untuk popup (Wajib ada biar FE aman)"""
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
    """SMART MODAL RECOMMENDATION berdasarkan visual size dan risk profile"""
    
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


def optimize_image_memory(image_path, max_size=800, quality=85):
    """OPTIMIZED: Resize image to reduce memory footprint and upload latency"""
    try:
        if "placeholder" in image_path or not os.path.exists(image_path):
            return None
            
        with Image.open(image_path) as img:
            # Convert to RGB if necessary
            if img.mode != 'RGB':
                img = img.convert('RGB')
                
            # Calculate new size maintaining aspect ratio
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            # Save to memory buffer as JPEG
            buffer = io.BytesIO()
            img.save(buffer, format='JPEG', quality=quality, optimize=True)
            buffer.seek(0)
            
            # Return Part object for Vertex AI
            return Part.from_data(data=buffer.getvalue(), mime_type="image/jpeg")
    except Exception as e:
        print(f"      ⚠️ Image optimization error for {image_path}: {e}")
        return None


def calculate_csv_metrics(batch_ids, cust_map, cust_loans, loan_loc):
    """Calculate group metrics from CSV data"""
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
    
    # Determine initial risk status from DPD
    if avg_dpd > 30:
        risk_status = "TOXIC"
    elif avg_dpd > 0:
        risk_status = "MEDIUM"
    else:
        risk_status = "HEALTHY"
    
    # Location logic
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
    
    return {
        "avg_dpd": avg_dpd,
        "total_loan": total_loan,
        "common_biz": common_biz,
        "risk_status": risk_status,
        "lat": lat,
        "lng": lng
    }


def generate_image_urls(group_counter, home_images, bisnis_images):
    """Generate image URLs for home and business images"""
    home_img_path = home_images[group_counter % len(home_images)]
    bisnis_img_path = bisnis_images[group_counter % len(bisnis_images)]
    
    CLOUD_RUN_URL = "https://socialcollateral-api-228221306168.asia-southeast2.run.app"
    
    if "placeholder" not in home_img_path and os.path.exists(home_img_path):
        home_img_url = f"{CLOUD_RUN_URL}/images/{os.path.basename(home_img_path)}"
    else:
        home_img_url = f"{CLOUD_RUN_URL}/images/placeholder_home.jpg"
        
    if "placeholder" not in bisnis_img_path and os.path.exists(bisnis_img_path):
        bisnis_img_url = f"{CLOUD_RUN_URL}/images/{os.path.basename(bisnis_img_path)}"
    else:
        bisnis_img_url = f"{CLOUD_RUN_URL}/images/placeholder_bisnis.jpg"
    
    return home_img_url, bisnis_img_url, home_img_path


def calculate_node_size(trust_score, risk_status):
    """
    Calculate visual node size based on trust score and risk status.
    HEALTHY: 25-50px (higher trust = bigger)
    MEDIUM: 18-28px (moderate)
    TOXIC: 20-45px (lower trust = bigger for urgency)
    """
    if risk_status == "HEALTHY":
        size = 25 + int((trust_score - 70) * 0.83)
        return max(25, min(50, size))
    elif risk_status == "MEDIUM":
        size = 18 + int((trust_score - 40) * 0.25)
        return max(18, min(28, size))
    else:  # TOXIC
        inverted_urgency = (60 - trust_score) if trust_score <= 60 else 0
        size = 20 + int(inverted_urgency * 0.5)
        return max(20, min(45, size))


def reconcile_risk_status(trust_score, initial_risk_status, group_id):
    """
    Reconcile risk status based on trust score.
    Priority: trust_score defines final risk classification
    """
    if trust_score >= 80:
        final_risk_status = "HEALTHY"
        final_node_color = "healthy"
    elif trust_score > 25:
        final_risk_status = "MEDIUM"
        final_node_color = "medium"
    else:
        final_risk_status = "TOXIC"
        final_node_color = "toxic"
    
    if final_risk_status != initial_risk_status:
        print(f"      ℹ️ Reconciling risk for {group_id}: {initial_risk_status} → {final_risk_status}")
    
    return final_risk_status, final_node_color


def get_ai_inference_with_retry(group_id, group_counter, avg_dpd, common_biz, total_loan, home_img_path):
    """Attempt AI inference with exponential backoff retry"""
    ai_data = {}
    
    if (
        group_counter < AI_LIMIT
        and GOOGLE_API_KEY != "MASUKKAN_API_KEY_ANDA_DISINI"
        and AI_AVAILABLE
    ):
        prompt = GROUP_ANALYSIS_PROMPT.format(
            group_text=f"ID: {group_id}, DPD: {avg_dpd}, Biz: {common_biz}, Loan: {total_loan}"
        )
        vertex_inputs = [prompt]
        
        optimized_image = optimize_image_memory(home_img_path)
        if optimized_image:
            vertex_inputs.append(optimized_image)
        
        max_retries, base_delay = 3, 2
        
        for attempt in range(max_retries):
            try:
                resp = model.generate_content(
                    vertex_inputs,
                    generation_config={
                        "max_output_tokens": 2048,
                        "temperature": 0.3,
                        "response_mime_type": "application/json"
                    }
                )
                ai_data = json.loads(resp.text)
                break
            except Exception as e:
                error_str = str(e).lower()
                is_quota_error = any(x in error_str for x in ["429", "resource exhausted", "quota", "rate limit"])
                
                if is_quota_error and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"      ⏳ {group_id} Rate limited (attempt {attempt + 1}/{max_retries}), retrying in {delay}s...")
                    time.sleep(delay)
                elif attempt == max_retries - 1:
                    print(f"      ⚠️ {group_id} AI failed after {max_retries} attempts: {e}")
                else:
                    print(f"      ⚠️ {group_id} AI Error (non-quota): {e}")
                    break
    
    return ai_data


def build_mock_data(risk_status, common_biz):
    """Build mock AI data when real AI is unavailable"""
    if risk_status == "HEALTHY":
        base_trust = random.randint(70, 95)
    elif risk_status == "MEDIUM":
        base_trust = random.randint(40, 75)
    else:
        base_trust = random.randint(10, 55)
    
    if base_trust >= 80:
        rec_text = f"🟢 LAYAK MODAL: Kelompok {common_biz} dapat diberikan modal standar. (Mock)"
    elif base_trust >= 50:
        rec_text = f"🟡 REVIEW DETAIL: Kelompok {common_biz} perlu evaluasi mendalam. (Mock)"
    else:
        rec_text = f"🔴 TIDAK LAYAK: Kelompok {common_biz} tidak direkomendasikan untuk modal. (Mock)"
    
    return {
        "risk_badge": f"{risk_status} RISK",
        "trust_score": base_trust,
        "sentiment_text": f"Kelompok didominasi usaha {common_biz}, performa pembayaran {risk_status.lower()}.",
        "asset_condition": "AVERAGE",
        "asset_tags": ["Usaha Mikro", "Bangunan Permanen"],
        "repayment_prediction": base_trust,
        "recommendation_text": rec_text,
    }


def build_group_data(group_id, group_counter, metrics, ai_data, home_img_url, bisnis_img_url, 
                     risk_status, node_color, node_size, batch_ids, is_error=False):
    """
    Build complete group data structure. 
    If is_error=True, returns fallback data with defaults.
    """
    if is_error:
        return {
            "id": group_id,
            "type": "medium",
            "size": 20,
            "x": random.randint(0, 1000),
            "y": random.randint(0, 1000),
            "lat": -6.59,
            "lng": 106.8,
            "header": {
                "name": generate_group_name(group_counter + 1),
                "location_city": "Jakarta Pusat",
                "location_village": "Desa Fallback",
                "member_count": len(batch_ids),
                "risk_badge": "MED RISK",
                "trust_score": 50,
                "loan_eligibility": "Review",
                "total_loan_amount": 0,
                "visual_priority": "LOW",
            },
            "overview": {"primary_driver": {"text": "Error during processing", "payment_score": 50, "social_score": 50}, "metrics": {"cycle": 1, "repayment_rate": 50, "avg_delay": "H+0"}, "neighbors": []},
            "trends": {"repayment_history": [], "asset_growth": [], "stats": {"streak": 0, "last_default": "Unknown", "trend_val": 0, "trend_dir": "flat", "avg_rate": 50.0, "best_rate": 50.0}, "seasonality_heatmap": [1] * 12},
            "insights": {"social_graph": {"risk_members": []}, "cv": {"home": {"condition": "UNKNOWN", "material": "Unknown", "roof": "Unknown", "access": "Unknown", "occupancy": "Unknown", "assets": [], "img_url": "placeholder_home.jpg"}, "biz": {"stability": "Unknown", "type": "Unknown", "traffic": "Unknown", "status": "Unknown", "digital": "Unknown", "inventory": [], "img_url": "placeholder_bisnis.jpg"}}, "prediction": {"default_risk_prob": 50, "horizon_days": 30, "what_if": {"current_score": 50, "projected_score": 50, "improvement_pct": 0, "scenario": "Error"}}, "recommendation_text": "Error during processing - manual review required"},
            "decision": {"last_audit": "System", "is_locked": True, "audit_date": datetime.now().strftime("%Y-%m-%d")}
        }
    
    trust_score = ai_data.get("trust_score", 70)
    location = generate_random_location()
    
    return {
        "id": group_id,
        "type": node_color,
        "size": node_size,
        "x": random.randint(0, 1000),
        "y": random.randint(0, 1000),
        "lat": metrics["lat"],
        "lng": metrics["lng"],
        "header": {
            "name": generate_group_name(group_counter + 1),
            "location_city": location["city"],
            "location_village": location["village"],
            "member_count": len(batch_ids),
            "risk_badge": ai_data.get("risk_badge", f"{risk_status} RISK"),
            "trust_score": trust_score,
            "loan_eligibility": "Eligible" if trust_score > 70 else "Review",
            "total_loan_amount": int(metrics["total_loan"]),
            "visual_priority": "HIGH" if (risk_status == "HEALTHY" and node_size >= 35) or (risk_status == "TOXIC" and node_size >= 30) else "MEDIUM" if node_size >= 20 else "LOW",
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
                "avg_delay": f"H+{int(metrics['avg_dpd'])}",
            },
            "neighbors": [],
        },
        "trends": {
            "repayment_history": generate_trend_data(trust_score, is_asset=False),
            "asset_growth": generate_trend_data(trust_score, is_asset=True),
            "stats": {
                "streak": random.randint(1, 12),
                "last_default": "Never" if metrics["avg_dpd"] == 0 else "Active",
                "trend_val": 2.5,
                "trend_dir": "up" if trust_score > 70 else "down",
                "avg_rate": 98.0,
                "best_rate": 100.0,
            },
            "seasonality_heatmap": [1, 1, 1, 2, 2, 3, 1, 1, 1, 1, 1, 1],
        },
        "insights": {
            "social_graph": {
                "risk_members": generate_risk_members(len(batch_ids), node_color)
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
                    "type": metrics["common_biz"],
                    "traffic": "Medium",
                    "status": "Active",
                    "digital": "QRIS",
                    "inventory": ["Full"],
                    "img_url": bisnis_img_url,
                },
            },
            "prediction": {
                "default_risk_prob": 100 - trust_score,
                "horizon_days": 30,
                "what_if": {
                    "current_score": trust_score,
                    "projected_score": min(100, trust_score + 5),
                    "improvement_pct": 5,
                    "scenario": "Intervention",
                },
            },
            "recommendation_text": ai_data.get("recommendation_text", "Rekomendasi dari AI tidak tersedia."),
        },
        "decision": {
            "last_audit": f"Agent {random.choice(['Budi', 'Sari'])}",
            "is_locked": True if risk_status == "TOXIC" else False,
            "audit_date": datetime.now().strftime("%Y-%m-%d"),
        },
    }


def build_neighbor_relationships(processed_groups, target_neighbors=5):
    """Build neighbor relationships for all groups based on geographic proximity"""
    gids = list(processed_groups.keys())
    city_map = {}
    village_map = {}
    
    for _gid, _data in processed_groups.items():
        city = _data.get("header", {}).get("location_city")
        village = _data.get("header", {}).get("location_village")
        city_map.setdefault(city, []).append(_gid)
        village_map.setdefault((city, village), []).append(_gid)
    
    for gid in processed_groups.keys():
        my_city = processed_groups[gid].get("header", {}).get("location_city")
        my_village = processed_groups[gid].get("header", {}).get("location_village")
        
        same_village_candidates = [x for x in village_map.get((my_city, my_village), []) if x != gid]
        
        neighbors = []
        if len(same_village_candidates) >= target_neighbors:
            neighbors = random.sample(same_village_candidates, k=target_neighbors)
        else:
            neighbors = same_village_candidates.copy()
            same_city_candidates = [x for x in city_map.get(my_city, []) if x != gid and x not in neighbors]
            need = 3 - len(neighbors)
            if same_city_candidates and need > 0:
                neighbors += random.sample(same_city_candidates, k=min(need, len(same_city_candidates)))
            
            need = target_neighbors - len(neighbors)
            if need > 0:
                others = [x for x in gids if x != gid and x not in neighbors]
                if others:
                    neighbors += random.sample(others, k=min(need, len(others)))
        
        processed_groups[gid]["overview"]["neighbors"] = []
        for nid in neighbors:
            n_data = processed_groups[nid]
            dist = random.randint(20, 500)
            
            if n_data.get("type") == "toxic":
                rel = "Risk Contagion"
            elif n_data.get("header", {}).get("location_village") == my_village:
                rel = "Same Village"
            elif n_data.get("header", {}).get("location_city") == my_city:
                rel = "Same City"
            else:
                rel = "Shared Agent"
            
            if dist < 100 and rel != "Risk Contagion":
                rel = "Geo-Cluster"
            
            processed_groups[gid]["overview"]["neighbors"].append({
                "id": nid,
                "name": n_data["header"]["name"],
                "risk": n_data["type"],
                "distance": f"{dist}m",
                "relation": rel,
            })


# PARALLEL WORKER FUNCTION
def process_single_group(args):
    """Process a single group using modular helper functions"""
    (
        group_counter, batch_ids, cust_map, cust_loans, loan_loc,
        home_images, bisnis_images, AI_AVAILABLE
    ) = args
    
    group_id = f"G{str(group_counter + 1).zfill(3)}"
    
    try:
        # Calculate metrics from CSV data
        metrics = calculate_csv_metrics(batch_ids, cust_map, cust_loans, loan_loc)
        
        # Generate image URLs
        home_img_url, bisnis_img_url, home_img_path = generate_image_urls(
            group_counter, home_images, bisnis_images
        )
        
        # Get AI inference or build mock data
        ai_data = get_ai_inference_with_retry(
            group_id, group_counter, metrics["avg_dpd"], 
            metrics["common_biz"], metrics["total_loan"], home_img_path
        )
        
        if not ai_data:
            ai_data = build_mock_data(metrics["risk_status"], metrics["common_biz"])
        
        # Reconcile risk status based on trust score
        trust_score = ai_data.get("trust_score", 70)
        risk_status, node_color = reconcile_risk_status(
            trust_score, metrics["risk_status"], group_id
        )
        
        # Calculate node size
        node_size = calculate_node_size(trust_score, risk_status)
        print(f"      📊 {group_id}: {risk_status} | Trust: {trust_score} → Size: {node_size}px")
        
        # Build group data structure
        group_data = build_group_data(
            group_id, group_counter, metrics, ai_data, 
            home_img_url, bisnis_img_url, risk_status, node_color, 
            node_size, batch_ids, is_error=False
        )
        
        print(f"✅ {group_id} Processed | Trust: {trust_score}")
        return group_data
        
    except Exception as e:
        print(f"❌ Error processing {group_id}: {e}")
        return build_group_data(
            group_id, group_counter, {}, {}, "", "", "", "", 0, batch_ids, is_error=True
        )



# MAIN PARALLEL PROCESSOR
def process_data():
    global AI_AVAILABLE
    print(f"🚀 MEMULAI PARALLEL SEEDING ({MAX_NODES} Nodes, {MAX_WORKERS} Workers with Rate Limiting)...")

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

    # 3. Prepare Image Lists
    all_cust_ids = list(cust_map.keys())
    
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

    # 4. Prepare Tasks for Parallel Processing
    tasks_args = []
    group_counter = 0

    for i in range(0, len(all_cust_ids), GROUP_SIZE):
        if group_counter >= MAX_NODES:
            break

        batch_ids = all_cust_ids[i : i + GROUP_SIZE]
        if not batch_ids:
            continue

        # Prepare arguments for worker function
        task_args = (
            group_counter, batch_ids, cust_map, cust_loans, loan_loc,
            home_images, bisnis_images, AI_AVAILABLE
        )
        tasks_args.append(task_args)
        group_counter += 1

    # 5. PARALLEL EXECUTION
    print(f"   🚀 Starting {len(tasks_args)} parallel tasks...")
    processed_groups = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_args = {executor.submit(process_single_group, args): args for args in tasks_args}
        
        # Collect results as they complete
        for future in as_completed(future_to_args):
            try:
                result = future.result()
                if result and "id" in result:
                    processed_groups[result["id"]] = result
            except Exception as exc:
                print(f'❌ Task generated an exception: {exc}')

    print(f"   ✅ Parallel processing complete! {len(processed_groups)} groups processed.")


    # Build neighbor relationships
    build_neighbor_relationships(processed_groups)

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
