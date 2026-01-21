# SocialCollateral AI - Backend Service (Competition Version)

> **Branch**: `mvp-hackathon`  
> This is the competition demo version from the Amartha x GDG Jakarta 2025 Hackathon. For baseline architecture, see [`main`](https://github.com/SocialCollateralAI/socialcollateral-ai/tree/main) branch. For GCP setup reference, see [`pre-hackathon`](https://github.com/SocialCollateralAI/socialcollateral-ai/tree/pre-hackathon) branch.

## Competition Context

**Event**: Amartha x Google Developer Groups Jakarta Hackathon 2025  
**Team**: Tim Suksemustanice  
**Achievement**: Top 15 Finalists  
**Timeline**: 24-hour development sprint  
**Status**: Historical documentation (competition concluded)

This branch preserves the actual code used during live competition, demonstrating social graph analytics for microfinance risk assessment in the Amartha (Grameen Bank) ecosystem.

## Data & Privacy Notice

⚠️ **Important**: This branch was developed with actual Amartha microfinance data during the competition:

| Data Type                   | Included in Repo? | Note                                                           |
| --------------------------- | ----------------- | -------------------------------------------------------------- |
| **CSV Source Files**        | ❌ No             | Excluded for data privacy                                      |
| **Member Profile Images**   | ❌ No             | Excluded for privacy & security                                |
| **Pre-computed AI Results** | ✅ Yes            | Sanitized output in `data/mock_db.json` and `data/mock_1.json` |

### How Data Was Processed

Results in this repository were generated using:

- **Google Vertex AI (Gemini)** - NLP sentiment analysis and group profiling
- **Pre-computed Pipeline** - Batch processing to work within GCP quota limits (not live API calls)
- **Custom Social Graph Algorithms** - Network analysis for risk assessment

**Current Status**: GCP credits have been exhausted and API keys are no longer active. This code is preserved for portfolio and documentation purposes.

## Technology Stack

- **Framework**: FastAPI (Python 3.9+)
- **AI/ML**: Google Vertex AI (Gemini GenAI)
- **Image Processing**: Pillow (for CV asset optimization)
- **Deployment**:
  - **Historical**: Railway (used during competition)
  - **Current**: Vercel (for documentation deployment)
- **Database**: JSON-based pre-computed storage

## Repository Structure

```
mvp-hackathon/
├── app/
│   ├── api/
│   │   ├── graph.py            # Sigma.js graph visualization endpoint
│   │   └── groups.py           # Group risk analysis endpoint
│   ├── models/
│   │   └── schemas.py          # Pydantic data models
│   ├── services/
│   │   └── data_service.py     # Data access layer
│   └── main.py                 # FastAPI application entry point
├── scripts/
│   └── intelligent_seeder.py   # GCP-integrated data generator (819 lines)
├── data/
│   ├── mock_db.json            # Pre-computed Amartha analysis results
│   └── mock_1.json             # Initial seed data
├── api/
│   └── index.py                # Vercel serverless entry point
├── vercel.json                 # Vercel deployment configuration
├── .vscode/
│   └── settings.json           # Workspace development settings
├── .gitignore                  # Privacy-safe ignore rules
├── requirements.txt            # Python dependencies (includes GCP)
└── README.md                   # This file
```

**Historical Files (Preserved)**:

- `Dockerfile` - Cloud Run containerization
- `railway.toml` - Railway deployment config (competition deployment)
- `.dockerignore`, `.gcloudignore` - Cloud deployment rules

## Development Challenges & Limitations

### Built in 24 Hours

The following limitations are **expected and documented**:

**Challenges Encountered**:

- New Amartha data structure required schema adjustments mid-development
- GCP API integration learning curve under time pressure
- Pre-computation pipeline design to work within quota limits
- Limited testing time for edge cases

**Known Limitations**:

- ❌ **GCP Credits Exhausted** - Cannot run live AI inference
- ❌ **API Keys Expired** - Hardcoded keys in `intelligent_seeder.py` are no longer active
- ⚠️ **Optimization Opportunities** - Some code could be more efficient
- ⚠️ **Error Handling** - Could be more robust for production use
- ⚠️ **Hardcoded Configuration** - Should use environment-based config

**Why This Matters**: This branch demonstrates **concept viability and technical feasibility**, not production readiness. It's a snapshot of what was built under competition constraints.

## API Endpoints

**Base URL (Current)**: `https://api.socialcollateral.id/api/v1`  
**Historical URL**: `https://socialcollateral-ai-production.up.railway.app` (no longer active)  
**Local Development**: `http://localhost:8000/api/v1`

| Method | Endpoint       | Description                                                               |
| ------ | -------------- | ------------------------------------------------------------------------- |
| `GET`  | `/graph`       | Returns complete node/edge graph data for Sigma.js visualization          |
| `GET`  | `/groups/{id}` | Returns detailed group analysis (Graph metrics, NLP sentiment, CV assets) |

**Interactive Documentation**: `/docs` (FastAPI auto-generated Swagger UI)

## Local Development

### Prerequisites

- Python 3.9 or higher
- pip package manager
- (Optional) GCP credentials for running seeder with live AI

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn app.main:app --reload

# Access at http://localhost:8000
# API docs at http://localhost:8000/docs
```

### Running the Seeder

⚠️ **Note**: The intelligent seeder requires active GCP credentials. Since competition credits are exhausted, the seeder will use mock fallback data.

```bash
python scripts/intelligent_seeder.py
```

**To run with live GCP inference** (requires setup):

1. Active GCP project with Vertex AI API enabled
2. Valid service account credentials
3. Sufficient API quota allocation
4. Set environment variables: `GCP_PROJECT_ID`, `GOOGLE_API_KEY`

## Deployment

### Current (Vercel)

**Production**: `https://api.socialcollateral.id`  
**Configuration**: `vercel.json` + `api/index.py`

Vercel deployment is used for documentation and portfolio purposes.

### Historical (Railway - Competition)

**URL**: `https://socialcollateral-ai-production.up.railway.app` (inactive)  
**Configuration**: `railway.toml` + `Dockerfile`

Railway was used during the actual 24-hour competition for rapid deployment.

## Honest Assessment

### ✅ What Works

- FastAPI endpoints are functional with pre-computed data
- Graph visualization data structure is complete
- Pre-computed AI analysis demonstrates the approach
- Deployment configurations (both Railway and Vercel) are preserved

### ⚠️ What Needs Improvement

- GCP dependencies now properly listed in `requirements.txt`
- Hardcoded credentials documented as expired/historical
- Error handling could be more comprehensive
- No live AI inference without valid GCP setup
- Some code comments were informal (now cleaned up)

### 🎯 Purpose

This is a **competition artifact** - a preserved snapshot of what was built in 24 hours under pressure with real-world data constraints. It serves as:

- Technical portfolio demonstration
- Learning reference for GCP Vertex AI integration
- Documentation of social graph analytics approach
- Example of hackathon MVP development

---

## License

Copyright © 2025 Tim Suksemustanice. All Rights Reserved.

This project is maintained as technical portfolio and research documentation. For commercial licensing or collaboration inquiries, contact the [team](https://www.linkedin.com/in/firyan-fatih-fadilah).
