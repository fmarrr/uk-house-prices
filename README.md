# UK House Price Paid dbt project
A dbt project modelling UK residential property transaction data from the HM Land Registry Price Paid dataset (2018–2025), built on Snowflake using dbt Fusion.

## Project purposes
 
This project has two purposes.
 
1) Analyse UK property price trends — London vs the rest of England and Wales — and identify areas with the strongest historical price growth as a guide for where to invest. The underlying data is open, granular, and covers a period dense with macroeconomic events, making it well-suited for this kind of retrospective analysis.
 
2) The second is as a portfolio project demonstrating end-to-end analytics engineering on open data: raw ingestion, staged transformation, and a mart layer ready for analysis — using dbt Fusion on Snowflake.
 
Both use cases can be extended further; the open data source and modular model structure make it straightforward to add new dimensions, regions, or time periods.

## Why 2018 - 2025?
The 7-year window was chosen partly for practical reasons (Snowflake free trial storage limits) but it also happens to cover one of the most eventful periods in the UK housing market: 
- **2018–2019** — post-referendum baseline; market uncertainty as Brexit negotiations dragged on
- **2020** — Brexit transition period ends; Covid-19 hits, transaction volumes collapse in Q2
- **2021** — stamp duty holiday drives a sharp rebound; prices surge across most regions
- **2022** — mini-budget and rapid interest rate rises cool the market abruptly
- **2023–2024** — sustained high mortgage rates suppress transaction volumes; regional divergence widens
- **2025** — gradual rate cuts begin; early signs of recovery

Taken together, this window captures boom, shock, and correction — giving the analysis real texture beyond a flat trend line.

## Data source

**HM Land Registry Price Paid Data**
- Coverage: England and Wales residential property transactions
- Period: 2018–2025
- Granularity: Individual transactions (address, price, date, property type, tenure, etc.)
- Source: [HM Land Registry Open Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
- Licence: Open Government Licence v3.0

## Analysis focus
 
- Price trends over time: London boroughs vs regions outside London
- Year-on-year price growth by district and county
- High-return areas: districts with sustained above-average appreciation across the full window
- Impact of macro events (stamp duty holiday, rate rises) on transaction volumes and prices by region
- Property type breakdown: whether flats vs houses show different growth trajectories in the same area

## Notes
 
- Category B transactions (repossessions, auctions, and other non-standard sales) are excluded at the staging layer to focus on open market transactions.
- Scotland and Northern Ireland are not covered by this dataset.
- Postcode-level granularity allows joining to ONS lookup tables for LSOA or region-level enrichment if needed.