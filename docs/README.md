# APJ Multi-Industry Predictive Maintenance Platform - Documentation

Professional technical documentation for the Databricks-powered predictive and prescriptive maintenance platform.

## Documentation Suite

### Technical Documentation

- **[Technical Overview for SAs](technical-overview-sa.html)** ([PDF](technical-overview-sa.pdf))
  - Complete architecture and technology stack
  - 5 industries, 20 APJ sites, multi-currency support
  - Databricks technology demonstration (Zerobus, DLT, MLflow, Lakebase, Genie)
  - Quick deployment guide (under 5 minutes)
  - Demo scenarios for customer meetings

- **[Technical Asset Inventory](technical-asset-inventory.html)** ([PDF](technical-asset-inventory.pdf))
  - Complete catalog of all components
  - DLT pipelines, ML modules, tables, jobs
  - Deployment artifacts and configuration files
  - Detailed component descriptions

### Executive Documentation

- **[Executive Overview for CXOs](executive-overview-cxo.html)** ([PDF](executive-overview-cxo.pdf))
  - Business case and ROI analysis
  - 25-35% downtime reduction, 15-25% cost savings
  - Multi-currency financial dashboards (AUD, JPY, INR, SGD, KRW)
  - Natural language analytics in English, Japanese, Korean
  - SFDC-aligned anchor accounts

- **[Finance Genie Adoption/ROI Validation (10Q) PDF](finance_genie_adoption_10q_professional.pdf)**
  - Cross-industry executive validation of platform adoption metrics
  - Model utilization, Genie usage, prediction coverage, and site maturity scoring
  - Cost per prediction, avoided cost per prediction, and platform ROI evidence
  - Q&A transcript captured exactly from Genie responses

### Developer Reference

- **[File Reference Index](file-reference-index.html)**
  - Searchable index of 100+ source files
  - Clickable repository links
  - Categorized by type (DLT, ML, App, Config, Tests)
  - Real-time search functionality

## Technology Stack

- **Zerobus** - First-mile OT/IoT connectivity (OPC-UA, Modbus, MQTT)
- **Delta Live Tables** - Medallion architecture (Bronze/Silver/Gold)
- **MLflow** - ML lifecycle management and model registry
- **Lakebase** - Operational OLTP for decision persistence
- **Genie AI** - Natural language query in multiple languages
- **Databricks Apps** - FastAPI + React application deployment
- **Asset Bundles** - Infrastructure-as-code

## Industry Coverage

| Industry | Anchor Account | APJ Sites | Use Case |
|----------|---------------|-----------|----------|
| Mining | Rio Tinto | 4 | Haul fleet and conveyor reliability |
| Energy | Alinta Energy | 4 | Wind, BESS, transformer availability |
| Water | Sydney Water | 4 | Pumping and leak-risk operations |
| Automotive | Toyota | 4 | Press and weld line stop-risk reduction |
| Semiconductor | Renesas | 4 | Etch and lithography yield protection |

## GitHub Pages Setup

This documentation is configured for GitHub Pages deployment:

1. **Enable GitHub Pages** in your repository settings:
   - Go to Settings > Pages
   - Source: Deploy from branch
   - Branch: `main`
   - Folder: `/docs`
   - Click Save

2. **Access Documentation**:
   - Your docs will be available at: `https://<username>.github.io/<repo-name>/`
   - Index page: `index.html`
   - All HTML and PDF files are directly accessible

3. **Configuration**:
   - Jekyll theme: minimal
   - Custom styling with Databricks brand colors
   - Responsive design for mobile and desktop

## Brand Colors

All documentation uses Databricks brand colors:

- **Primary Navy**: `#1b2431` (headers, text)
- **Primary Red**: `#ff3621` (accents, CTAs)
- **Secondary Navy**: `#334155` (gradients)
- **Background**: `#f0f2f5` (light gray)
- **Borders**: `#e2e8f0` (light gray)

## Development

To update documentation:

1. Edit HTML files in `/docs` directory
2. Regenerate PDFs using headless Chrome if needed
3. Commit and push to `main` branch
4. GitHub Pages will automatically rebuild

## License

Copyright 2024 Databricks Inc.
