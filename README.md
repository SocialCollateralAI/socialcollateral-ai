# SocialCollateral AI – Backend Engine (FastAPI)

**Team Name:** Tim suksemustanice  
Submission untuk **Amartha x GDG Jakarta Hackathon 2025**

Backend ini merupakan “Brain” dari sistem SocialCollateral AI yang menghitung *Group Trust Score* menggunakan Social Graph Analysis, NLP, dan Computer Vision.

---

## 🔗 Deployment & Live Demo

| Service | URL |
|--------|-----|
| **Backend API** | https://socialcollateral-ai-production.up.railway.app |
| **API Docs (Swagger)** | https://socialcollateral-ai-production.up.railway.app/docs |

---

## 🚀 Tech Stack

- **FastAPI** – REST API Framework  
- **Google Gemini** – Social Graph Analysis  
- **Google Gemini** – NLP Metric (sentimen & trust)  
- **Google Vision API** – CV Metric (verifikasi foto aset)  
- **Uvicorn** – ASGI Web Server  

---

## 📂 Repository Structure

```
socialcollateral-ai/                    # Backend Repository (Python FastAPI)
├── app/
│   ├── api/
│   │   ├── graph.py                   # Endpoint untuk data visualisasi graf (Sigma.js)
│   │   └── groups.py                  # Endpoint untuk detail profil risiko grup
│   ├── models/
│   │   └── schemas.py                 # Definisi skema data (Pydantic models)
│   ├── services/
│   │   └── data_service.py            # Logika pengambilan data & integrasi Mock DB
│   └── main.py                        # Entry point aplikasi FastAPI
├── data/
│   ├── mock_1.json                    # Data seed awal
│   └── mock_db.json                   # Database utama (hasil generate)
├── scripts/
│   └── intelligent_seeder.py          # Script generator data pintar (The Brain)
├── .dockerignore                      # Docker ignore rules
├── .gcloudignore                      # Google Cloud ignore rules
├── .gitignore                         # Git ignore rules
├── Dockerfile                         # Konfigurasi container Docker
├── railway.toml                       # Konfigurasi deployment Railway
├── README.md                          # Dokumentasi khusus Backend
└── requirements.txt                   # Daftar dependensi Python
```

---

## ⚙️ Installation & Setup

### Prerequisites
- Python 3.10 atau lebih tinggi
- Google Cloud API credentials (Gemini & Vision API)
- pip untuk package management

### Quick Start

```bash
# Clone repository
git clone https://github.com/SocialCollateralAI/socialcollateral-ai.git
cd socialcollateral-ai

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# atau
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Generate mock data (optional)
python scripts/intelligent_seeder.py

# Run development server
uvicorn app.main:app --reload

# Server berjalan di http://localhost:8000
# API Docs di http://localhost:8000/docs
```

---

## 🔐 Environment Variables

Buat file `.env` di root directory:

```env
# Google AI API Keys
GOOGLE_GEMINI_API_KEY=your_gemini_api_key_here
GOOGLE_VISION_API_KEY=your_vision_api_key_here

# Application Settings
APP_NAME=SocialCollateral AI
APP_VERSION=1.0.0
DEBUG=True

# CORS Settings (untuk Frontend)
ALLOWED_ORIGINS=https://socialcollateral-web.vercel.app,http://localhost:5173

# Database Path
MOCK_DB_PATH=data/mock_db.json
```

