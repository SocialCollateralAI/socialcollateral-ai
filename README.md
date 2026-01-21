# SocialCollateral AI - Backend & AI Service (Pre-Hackathon Prep)

> **Branch**: `pre-hackathon`  
> This branch contains GCP integration preparation code from the night before competition day. For baseline architecture, see [`main`](https://github.com/SocialCollateralAI/socialcollateral-ai/tree/main) branch.

## Branch Purpose

This is the preparation branch created before the live hackathon, containing:

- GCP service configuration and setup
- Vertex AI/Gemini integration scaffolding
- Pre-computed data processing pipeline
- Test scripts for GCP workflow validation

**Status**: Preparation code (not used in final competition demo)

## What's New vs Main Branch

**Additional Files**:

- `scripts/final_seeder.py` - GCP-ready seeder with Gemini integration
- `scripts/sample.py` - GCP workflow examples
- `app/sample.py` - Static file serving setup

**Modified Files**:

- `app/main.py` - GCP configuration hooks
- `data/mock_db.json` - Sample pre-computed results
- `requirements.txt` - Added GCP dependencies

---

**Team**: Tim Suksemustanice  
**Competition**: Amartha x GDG Jakarta Hackathon 2025
