# SocialCollateral AI - Backend & AI Service

Backend API service for **Jaringan Amanah** (The Amanah Network), a Social Graph Engine built for the Amartha ecosystem. Provides RESTful endpoints for network visualization and multi-perspective risk analysis.

## Repository Structure

This repository maintains three distinct branches representing different stages of the project lifecycle:

### Branch Overview

<table>
<thead>
  <tr>
    <th></th>
    <th><code>main</code></th>
    <th><code>pre-hackathon</code></th>
    <th><code>mvp-hackathon</code></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><strong>Description</strong></td>
    <td valign="top">Baseline Architecture</td>
    <td valign="top">GCP Integration Prep</td>
    <td valign="top">Competition Demo Version</td>
  </tr>
  <tr>
    <td><strong>Scope</strong></td>
    <td valign="top">
      • Initial schema and intelligent seeder implementation<br><br>
      • Matches technical specification from original proposal<br><br>
      • Uses Unsplash API for placeholder images<br><br>
      • Clean foundation for POC (Proof of Concept)
    </td>
    <td valign="top">
      • Pre-competition preparation branch (night before demo day)<br><br>
      • Includes GCP service configuration<br><br>
      • Setup for Vertex AI/Gemini integration<br><br>
      • Pre-computed data processing pipeline
    </td>
    <td valign="top">
      • Production demo used during live competition<br><br>
      • Integrated with actual Amartha data (CSV + images)<br><br>
      • Full GCP implementation (Vertex AI, Cloud Vision, pre-computed inference<br><br>
      • Minor schema additions to accommodate real-world data requirements
    </td>
  </tr>
</tbody>
</table>

> **Development Context**: This branch represents a functional MVP developed under hackathon time constraints (24 hours), optimized for demonstration rather than production deployment.

> **Why not use `mvp-hackathon` as main?**  
> The competition version relied on GCP credits which have been exhausted. This branch structure preserves the complete evolution from concept to implementation, serving as comprehensive technical documentation.

### Branch Selection Guide

- **For architecture reference**: `main`
- **For GCP setup study**: `pre-hackathon`
- **For complete implementation**: `mvp-hackathon`

## API Endpoints

**Production URL**: `https://api.social-collateral.id/api/v1`  
**Local URL**: `http://localhost:8000/api/v1`

| Method | Endpoint       | Description                                                               |
| ------ | -------------- | ------------------------------------------------------------------------- |
| `GET`  | `/graph`       | Returns complete node/edge graph data for Sigma.js visualization          |
| `GET`  | `/groups/{id}` | Returns detailed group analysis (Graph metrics, NLP sentiment, CV assets) |

**Interactive Documentation**: `api.social-collateral.id/docs` (FastAPI auto-generated Swagger UI)

<!--
## Technology Stack

- **Framework**: FastAPI (Python 3.9+)
- **Data Processing**: Intelligent seeder with schema validation
- **Deployment**: Vercel (Serverless Functions)
- **Database**: JSON-based mock storage (POC phase)
---
-->

<!--
## Local Development

### Prerequisites

- Python 3.9 or higher
- pip package manager

### Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Generate mock database**

   ```bash
   python scripts/intelligent_seeder.py
   ```

   This creates `data/mock_db.json` with schema-compliant sample data.

3. **Run development server**

   ```bash
   uvicorn app.main:app --reload
   ```

4. **Access API documentation**
   ```
   http://localhost:8000/docs
   ```
---
-->

<!--
## Deployment

### Vercel Setup

This repository is configured for Vercel deployment via `vercel.json`.

**Custom Domain**: `api.socialcollateral.id`

**Deployment Steps**:

1. Import repository to Vercel dashboard
2. Configure project settings (auto-detected from `vercel.json`)
3. Add custom domain in project settings
4. Update DNS provider with CNAME record

**Environment**: Production deployments auto-trigger from `main` branch.
---
-->

<!--
## Project Context

This backend was developed for a national-scale fintech hackathon, reaching the Top 15 finalists. The project demonstrates a novel approach to microfinance risk assessment using social graph analytics, natural language processing, and computer vision—all applied to the Grameen Bank framework implemented by Amartha.

**Competition**: Amartha x Google Developer Groups Jakarta Hackathon 2025
**Team**: Tim Suksemustanice
-->

---

## License

Copyright © 2025 Tim Suksemustanice. All Rights Reserved.

This project is maintained as a technical portfolio and research documentation. The code, algorithms, and system architecture are proprietary intellectual property.

For commercial licensing, collaboration, or technical inquiries, please contact the [team](https://www.linkedin.com/in/firyan-fatih-fadilah).

See [LICENSE](./LICENSE) for full terms.
