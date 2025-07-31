# DMC Compliance Charges – Snowpark Implementation Handbook

**Author**: Ameya Deshmukh  
**Date**: July 31, 2025  
**Version**: 1.4

## Overview

This handbook documents the Snowpark-based Python implementations of DMC compliance chargeback rules previously built in KNIME. The scripts automate compliance checks and apply chargebacks across key scenarios.

### Covered Rules

- **DMC 702** – Expedited Shipment Chargeback  
- **DMC 706** – Bill of Lading Label Violation  
- **DMC 710** – SLA Violation for Cancelled Orders  
- **Combined Aggregation** – Unified output across chargebacks

Each rule is implemented as a Python stored procedure with a Snowflake Task that runs weekly for the previous Sun–Sat window (America/Chicago).

## Repository Structure

- `run_dmc702_chargeback` – Implements DMC 702 logic
- `run_dmc706_chargeback` – Processes offshore BOL violations
- `run_dmc710_chargeback` – SLA-based cancellation audit
- `build_combined_chargebacks` – Merges all outputs for Finance

## Scheduling

Tasks run every Monday morning:
- `08:00` → DMC 702, 706, 710 scripts  
- `09:00` → Combined aggregation

## Maintenance Notes

- Inputs: EDW/OMNICHANNEL tables + offshore files  
- Constants: Rule IDs, chargeback rates, ship node exclusions  
- Output tables: `DMC<rule>_OUT`, aggregated to `DMC_CHARGEBACK_COMBINED`  
- Monitor: Task history & row-level audits

---

_For full technical details, refer to the complete implementation handbook._
