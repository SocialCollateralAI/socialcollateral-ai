# SocialCollateral AI - Backend & AI Service (Competition Version)

> **Branch**: `mvp-hackathon`  
> This is the competition demo version from the Amartha x GDG Jakarta 2025 Hackathon. For baseline architecture, see [`main`](https://github.com/SocialCollateralAI/socialcollateral-ai/tree/main) branch. For GCP setup reference, see [`pre-hackathon`](https://github.com/SocialCollateralAI/socialcollateral-ai/tree/pre-hackathon) branch.

## Branch Purpose

This branch preserves the production code from the 24-hour hackathon, containing:

- Full GCP integration (Vertex AI/Gemini for NLP analysis)
- Real Amartha microfinance data processing (CSV + images)
- Pre-computed AI inference results
- Railway deployment configuration (historical)

**Status**: Historical documentation (GCP credits exhausted, API keys expired)

## Development Challenges & Limitations

### Built in 24 Hours

The following limitations are **expected and documented**:

**Challenges Encountered**:

- New Amartha data structure required schema adjustments mid-development
- GCP API integration learning curve under time pressure
- Pre-computation pipeline design to work within quota limits
- Limited testing time for edge cases

**Known Limitations**:

- ✘ **GCP Credits Exhausted** - Cannot run live AI inference
- ✘ **API Keys Expired** - Hardcoded keys in `intelligent_seeder.py` are no longer active
- ⚠︎ **Optimization Opportunities** - Some code could be more efficient (see refactoring note below)
- ⚠︎ **Error Handling** - Could be more robust for production use

**Post-Competition Cleanup**: After the hackathon, the codebase was cleaned up with basic refactoring (extracted helper functions, removed code duplication, improved code organization) to make it more readable for portfolio purposes while preserving the original functionality.

## Data & Privacy Notice

**Important**: This branch was developed with actual Amartha microfinance data during the competition:

| Data Type                   | Included in Repo? | Note                                                           |
| --------------------------- | ----------------- | -------------------------------------------------------------- |
| **CSV Source Files**        | ✘ No              | Excluded for data privacy                                      |
| **Member Profile Images**   | ✘ No              | Excluded for privacy & security                                |
| **Pre-computed AI Results** | ✓ Yes             | Sanitized output in `data/mock_db.json` and `data/mock_1.json` |

### How Data Was Processed

Results in this repository were generated using:

- **Google Vertex AI (Gemini)** - NLP sentiment analysis and group profiling
- **Pre-computed Pipeline** - Batch processing to work within GCP quota limits
- **Custom Social Graph Algorithms** - Network analysis for risk assessment

**Current Status**: GCP credits exhausted and API keys expired. Code preserved for portfolio and documentation purposes.

## What's New vs Main Branch

**Additional Files**:

- `scripts/intelligent_seeder.py` - GCP-integrated seeder with Vertex AI
- `api/index.py` - Vercel serverless entry point
- `vercel.json` - Vercel deployment configuration
- `.vscode/settings.json` - Development workspace settings

**Modified Files**:

- `app/main.py` - Image serving endpoints for Amartha data
- `data/mock_db.json` - Pre-computed Amartha analysis results
- `requirements.txt` - Added GCP dependencies (google-cloud-aiplatform, pillow)
- `.gitignore` - Privacy-safe ignore rules

**Historical Files (Preserved)**:

- `Dockerfile`, `.dockerignore`, `.gcloudignore` - Cloud Run/GCP deployment configs

<!-- **Team**: Tim Suksemustanice
**Competition**: Amartha x GDG Jakarta Hackathon 2025 -->
