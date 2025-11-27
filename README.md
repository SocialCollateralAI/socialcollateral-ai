# SocialCollateral AI - Backend Engine (MVP)

Backend service berbasis FastAPI yang berfungsi sebagai "Otak Risiko" untuk aplikasi Jaringan Amanah Amartha.

Menyediakan API untuk Visualisasi Graf (Sigma.js) dan Analisis Multi-Lensa (NLP, CV, Graph Metrics).

  

## READY FOR DEPLOYMENT

### Repositori Struktur

-   `app/` : Kode utama API (Endpoints & Logic).
    
-   `scripts/` : **The Brain**. Data Generator (`intelligent_seeder.py`).
    
-   `data/` : Database JSON hasil generate.
    

  

## API Endpoints

Base URL Lokal: http://localhost:8000/api/v1

Base URL Cloud: https://socialcollateral-ai-production.up.railway.app/api/v1

Base URL Swagger / API Docs: https://socialcollateral-ai-production.up.railway.app/docs

| **Method** | **Endpoint** | **Deskripsi** |
| --- | --- | --- |
| `GET` | `/graph` | Mengembalikan Nodes & Edges lengkap untuk Sigma.js. |
| `GET` | `/groups/{id}` | Mengembalikan detail lengkap (Risk, NLP, CV) untuk Popup Dashboard. |

## Setup & Run (Lokal)

### 1\. Install Dependencies

Pastikan Python 3.9+ sudah terinstall.

    pip install -r requirements.txt
    

### 2\. Generate MOCK DB (WAJIB)

Jalankan script ini setiap kali ingin me-reset data atau mengubah narasi.

    python scripts/intelligent_seeder.py
    

_Output: Cek folder `data/`, pastikan file `mock_db.json` terupdate._

### 3\. Run Server API

    uvicorn app.main:app --reload
    

### 4\. Akses Swagger UI

Buka browser: **`http://localhost:8000/docs`**

## ☁️ Cara Deploy (Render/Railway)

1.  Pastikan `requirements.txt` ada di root folder.
    
2.  Pastikan `mock_db.json` sudah ter-generate dan ada di repo (jangan di gitignore).
    
3.  **Start Command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
    

**Tim Suksemustanice - Amartha x GDG Jakarta Hackathon 2025**
