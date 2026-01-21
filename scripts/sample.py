"""
GCP-integrated seeding script for SocialCollateral AI demo.

This script demonstrates multimodal AI analysis using Google Gemini for:
- CSV data processing from Amartha microfinance records
- Computer Vision analysis on member profile images
- Risk assessment and sentiment analysis
- Social graph generation

Designed for rapid prototyping during hackathon preparation.
"""

import csv
import glob
import json
import os
import random
import time

import google.generativeai as genai
from PIL import Image

# Configuration - Update these before running
GOOGLE_API_KEY = ""
INPUT_CSV_FILE = "raw_data/sample_data_amartha.csv"
INPUT_IMAGE_DIR = "data/images"
OUTPUT_JSON = "data/mock_db.json"

# Initialize Gemini API
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# Multimodal AI prompt for combined NLP + Vision analysis
MULTIMODAL_PROMPT = """
Act as a Senior Risk Analyst for Microfinance in Indonesia.

You will receive:
1. TEXT DATA: Borrower profile from CSV
2. IMAGE: Photo of their home/business asset

Task: Analyze risk holistically and output pure JSON.

Analysis required:
1. **Risk Badge**: Determine "LOW RISK", "MED RISK", or "HIGH RISK"
2. **Trust Score**: Provide score 0-100
3. **Sentiment Analysis**: One-sentence summary (Bahasa Indonesia)
4. **Asset Condition**: Rate asset in photo ("GOOD", "AVERAGE", "POOR")
5. **Asset Tags**: Provide 2-3 short visual tags

JSON Output Format (strict, no markdown):
{
  "risk_badge": "LOW RISK",
  "trust_score": 85,
  "sentiment_text": "description here",
  "asset_condition": "GOOD",
  "asset_tags": ["Tag1", "Tag2"],
  "repayment_prediction": 98
}
"""


def get_csv_value(row, keywords, default):
    """
    Intelligently map CSV columns by keyword matching.
    
    Args:
        row: CSV row dict
        keywords: List of possible column name keywords
        default: Fallback value if no match
        
    Returns:
        Matched column value or default
    """
    for key in row.keys():
        if any(k in key.lower() for k in keywords):
            return row[key]
    return default


def process_data():
    """Main data processing pipeline with AI integration."""
    
    print("Starting intelligent seeding with GCP integration...")
    
    # Validate input files
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"ERROR: CSV file not found at {INPUT_CSV_FILE}")
        print("Create 'raw_data' folder and add CSV file there.")
        return
    
    # Load available images
    all_images = (
        glob.glob(f"{INPUT_IMAGE_DIR}/*.jpg")
        + glob.glob(f"{INPUT_IMAGE_DIR}/*.jpeg")
        + glob.glob(f"{INPUT_IMAGE_DIR}/*.png")
    )
    
    if not all_images:
        print(f"WARNING: No images found in {INPUT_IMAGE_DIR}")
        all_images = ["placeholder.jpg"]
    
    groups = {}
    
    with open(INPUT_CSV_FILE, mode="r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        print(f"CSV columns detected: {headers}")
        
        for i, row in enumerate(reader):
            # Limit for demo purposes
            if i >= 50:
                break
            
            gid = f"G{str(i + 1).zfill(3)}"
            
            # Intelligent CSV mapping
            name = get_csv_value(row, ["nama", "name", "ketua"], f"Ibu {gid}")
            village = get_csv_value(row, ["desa", "village", "kelurahan"], "Desa Parung")
            loan = get_csv_value(row, ["plafon", "amount", "pinjaman"], "5000000")
            
            # Image selection - cycle through available images
            if all_images and all_images[0] != "placeholder.jpg":
                img_path = all_images[i % len(all_images)]
                img_filename = os.path.basename(img_path)
                img_url = f"http://localhost:8000/static/{img_filename}"
            else:
                img_path = None
                img_url = "https://images.unsplash.com/photo-1568605114967-8130f3a36994"
            
            print(f"Processing {gid} - {name} | Image: {img_filename if img_path else 'None'}...")
            
            # AI Analysis via Gemini
            ai_result = {}
            try:
                row_text = str(row)
                inputs = [MULTIMODAL_PROMPT, f"BORROWER DATA:\n{row_text}"]
                
                if img_path:
                    img_file = Image.open(img_path)
                    inputs.append(img_file)
                
                response = model.generate_content(
                    inputs, generation_config={"response_mime_type": "application/json"}
                )
                ai_result = json.loads(response.text)
                
            except Exception as e:
                print(f"   AI Error (Quota/Network): {e}")
                print("   Using fallback logic")
                # Fallback if API fails
                ai_result = {
                    "risk_badge": random.choice(["LOW RISK", "MED RISK"]),
                    "trust_score": random.randint(60, 95),
                    "sentiment_text": "AI analysis pending, historical data shows stable pattern.",
                    "asset_condition": "AVERAGE",
                    "asset_tags": ["Unverified"],
                    "repayment_prediction": 95,
                }
                time.sleep(1)
            
            # Map AI results to database schema
            node_type = "healthy"
            if ai_result["risk_badge"] == "HIGH RISK":
                node_type = "toxic"
            elif ai_result["risk_badge"] == "MED RISK":
                node_type = "medium"
            
            # Construct group node data
            groups[gid] = {
                "id": gid,
                "type": node_type,
                "x": random.randint(0, 1000),
                "y": random.randint(0, 1000),
                "header": {
                    "name": name.upper(),
                    "location_city": "Bogor",
                    "location_village": village,
                    "member_count": random.randint(10, 25),
                    "risk_badge": ai_result["risk_badge"],
                    "trust_score": ai_result["trust_score"],
                    "loan_eligibility": "Eligible"
                    if ai_result["trust_score"] > 70
                    else "Review",
                    "total_loan_amount": int(
                        str(loan).replace(".", "").replace(",", "")
                    ),
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
                    "neighbors": [],
                    "max_plafon_recommendation": 0
                    if node_type == "toxic"
                    else 50000000,
                },
                "trends": {
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
                    "social_graph": {"risk_members": []},
                    "cv": {
                        "home": {
                            "condition": ai_result["asset_condition"],
                            "material": "Verified",
                            "roof": "Verified",
                            "access": "Paved",
                            "occupancy": "Occupied",
                            "assets": ai_result["asset_tags"],
                            "img_url": img_url,
                        },
                        "biz": {
                            "stability": "Permanent",
                            "type": "Warung",
                            "traffic": "High",
                            "status": "Active",
                            "digital": "QRIS",
                            "inventory": ["Full Stock"],
                            "img_url": img_url,
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
            
            time.sleep(1)  # Rate limit protection
    
    # Generate social graph connections
    print("Generating social graph connections...")
    node_ids = list(groups.keys())
    
    for gid, data in groups.items():
        potential_neighbors = random.sample(node_ids, k=3)
        my_village = data["header"]["location_village"]
        
        neighbor_list = []
        for nid in potential_neighbors:
            if nid == gid:
                continue
            
            n_data = groups[nid]
            n_village = n_data["header"]["location_village"]
            
            # Determine relation type
            rel_type = "Shared Field Agent"
            if my_village == n_village:
                rel_type = "Geo-Cluster (< 50m)"
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
    
    # Save to JSON
    final_db = {
        "meta": {"version": "v-final-onsite", "total": len(groups)},
        "global_state": {
            "wallet_balance": 1000000000,
            "spending_history": [100, 200, 300],
        },
        "groups": groups,
    }
    
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(final_db, f, indent=2)
    
    print(f"SUCCESS! MVP database with AI analysis saved to: {OUTPUT_JSON}")


if __name__ == "__main__":
    process_data()
