# 🎬 dbt Cinema Analytics

**Advanced data pipeline for cinema performance analysis and cost-effectiveness optimization**

[![dbt](https://img.shields.io/badge/dbt-1.6+-orange.svg)](https://docs.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-blue.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Project Overview

This project demonstrates advanced **data engineering and analytics** skills through a comprehensive dbt pipeline that analyzes the efficiency of three movie theater locations in New Jersey. The solution integrates revenue and cost data from multiple sources with different formats to enable data-driven decision making for cinema management.

### 🎯 Business Challenge
- **Objective**: Analyze relationship between movie rental costs and revenue across 3 theater locations
- **Data Sources**: 5 different data sources with inconsistent formats and structures
- **Key Challenge**: Unify transaction, daily sales, and invoice data into a single analytical table
- **Outcome**: Monthly performance analysis enabling cost-effectiveness optimization

## 🏗️ Technical Architecture

### Data Pipeline Design
```
Raw Sources → Staging → Intermediate → Analytics Mart
     ↓           ↓           ↓            ↓
  5 Sources → Standardize → Business → Final Analysis
              & Clean      Logic      Table (165 rows)
```

### Key Technical Achievements

🔹 **Complex Data Integration**: Successfully unified 3 location datasets with different granularities:
- NJ_001: Transaction-level data → Monthly aggregation  
- NJ_002: Daily sales data → Monthly aggregation
- NJ_003: Multi-product transactions → Filtered & aggregated

🔹 **Smart Duplicate Resolution**: Identified and resolved invoice duplicates (159→144 records) using MAX aggregation strategy

🔹 **Revenue-First Strategy**: Implemented LEFT JOIN to preserve all revenue records while transparently handling missing cost data

🔹 **Custom Test Development**: Built reusable `composite_key_candidate` generic test for business key validation

## 📊 Final Analytics Table

| Column | Description | Business Value |
|--------|-------------|----------------|
| `movie_id` | Unique movie identifier | Movie performance tracking |
| `movie_title` | Human-readable name | Reporting & analysis |
| `genre` | Movie category | Genre performance analysis |
| `studio` | Production studio | Studio relationship analysis |
| `month` | Performance month | Trend analysis |
| `location` | Theater location (NJ_001/002/003) | Location comparison |
| `rental_cost` | Monthly studio fees | Cost analysis |
| `tickets_sold` | Monthly ticket sales | Volume metrics |
| `revenue` | Monthly ticket revenue | Performance metrics |
| `profit` | Monthly profit (Revenue - Rental Cost) | Profitability analysis |
| `roi` | Return on Investment (Profit / Rental Cost) | Investment efficiency |

**Granularity**: One record per movie, per location, per month
**Coverage**: 87.3% data completeness (144/165 records with complete cost data)

## 🛠️ Technical Implementation

### Data Quality Solutions
- **Missing Genres**: Replaced NULLs with 'Unknown' for complete categorization
- **Case Sensitivity**: Standardized location IDs (nj_001 → NJ_001)  
- **Date Harmonization**: Unified different date formats across sources
- **Product Filtering**: Extracted ticket-only transactions from multi-product data

### Advanced dbt Features

**🔧 Custom Generic Test**: `composite_key_candidate`
```sql
{% test composite_key_candidate(model, columns) %}
    select
        {% for column in columns %} {{ column }}{{ ',' if not loop.last }} {% endfor %},
        count(*) as duplicate_count
    from {{ model }}
    group by {% for column in columns %} {{ column }}{{ "," if not loop.last }} {% endfor %}
    having count(*) > 1
{% endtest %}
```

**🔧 Cross-Source Date Validation**
- Custom singular test ensuring final table dates exist in all source systems
- Prevents data pipeline errors from creating impossible date ranges

**🔧 Comprehensive Testing Strategy**
- Source data quality tests (warnings for known issues)
- Staging data validation (errors for pipeline failures)  
- Business logic verification (custom composite key tests)

## 📈 Business Impact & Analytics Capabilities

### Key Performance Metrics
- **Profitability Analysis**: Revenue - Rental Costs per movie/location
- **ROI Calculation**: (Revenue - Costs) / Costs × 100
- **Cost Efficiency**: Cost per ticket, Revenue per ticket
- **Location Comparison**: Performance benchmarking across NJ theaters

### Data-Driven Insights Enabled
- 🎯 **Movie Programming**: Identify highest ROI movies for future scheduling
- 🎯 **Location Optimization**: Compare efficiency across three theaters  
- 🎯 **Studio Negotiations**: Cost vs. performance data for contract discussions
- 🎯 **Trend Analysis**: Monthly performance tracking and forecasting

## 🔧 Technical Stack

- **Data Warehouse**: Snowflake
- **Transformation**: dbt Cloud
- **Version Control**: Git/GitHub
- **CI/CD**: dbt Cloud Jobs
- **Documentation**: dbt docs
- **Testing**: Custom + built-in dbt tests

## 🚀 Getting Started

### Prerequisites
- Snowflake account with appropriate permissions
- dbt Cloud account or dbt Core installation
- Access to source data tables

### Installation
```bash
# Clone repository
git clone https://github.com/MWetz84/Silverscreen
cd Silverscreen

# Install dependencies (if using dbt Core)
dbt deps

# Configure connection in profiles.yml
# Update sources in models/staging/_sources.yml

# Run pipeline
dbt build
```

### Running Tests
```bash
# Run all tests
dbt test

# Run custom tests only
dbt test --select test_type:singular

# Run composite key validation
dbt test --select composite_key_candidate
```

## 📁 Project Structure

```
models/
├── staging/           # Source data cleaning & standardization
│   ├── stg_movie_catalogue.sql
│   ├── stg_invoices.sql
│   ├── stg_nj_001.sql
│   ├── stg_nj_002.sql
│   └── stg_nj_003.sql
├── intermediate/      # Business logic & data integration
│   ├── int_invoices_monthly_costs.sql
│   ├── int_revenue.sql
│   └── int_all_locations_revenue.sql
└── marts/            # Final analytics tables
    └── mart_movies_efficiency.sql

tests/
└── singular/         # Custom business logic tests
    └── test_date_range.sql

macros/
└── composite_key.sql  # Reusable test macro
```

## 🏆 Key Achievements

✅ **Data Integration**: Successfully unified 5 heterogeneous data sources  
✅ **Quality Assurance**: Implemented comprehensive testing strategy  
✅ **Performance Optimization**: Resolved duplicate data issues efficiently  
✅ **Business Value**: Enabled cost-effectiveness analysis for cinema operations  
✅ **Code Quality**: Professional documentation and reusable components  
✅ **Production Ready**: Successfully deployed with CI/CD pipeline  

## 📋 Skills Demonstrated

### Data Engineering
- Complex ETL pipeline design and implementation
- Data quality assessment and remediation
- Multi-source data integration and harmonization
- Performance optimization and duplicate resolution

### dbt Development  
- Advanced macro development (custom generic tests)
- Layered model architecture (staging/intermediate/marts)
- Comprehensive testing strategies
- Professional documentation practices

### Business Analysis
- Revenue and cost analysis methodologies
- KPI development and metric calculation
- Stakeholder-focused data presentation
- Business continuity through transparent data handling

## 📞 Contact & Collaboration

**Looking for opportunities in data engineering and analytics!**

- 📧 Email: m.wetzel-auma@t-online.de
- 💼 LinkedIn: https://www.linkedin.com/in/wetzelmich/
- 🐱 GitHub: https://github.com/MWetz84

---

*This project demonstrates production-ready data engineering skills including advanced dbt development, complex data integration, and business-focused analytics design. The solution successfully handles real-world data challenges while maintaining code quality and business continuity.*
