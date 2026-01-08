# E-Commerce Clickstream Funnel Revenue Analytics using-SQL
SQL-based e-commerce analytics project analyzing clickstream behavior, funnel conversion, drop-offs, and revenue attribution by traffic source using session-level data.

## Project Overview
This project performs end-to-end e-commerce analytics using SQL Server, combining clickstream event data, session-level funnel analysis, and transactional revenue data.
The goal is to understand:
* How users navigate through the e-commerce funnel
* Where drop-offs occur
* Which sessions convert into actual transactions
* How traffic sources perform in terms of conversion and revenue

## Data Model & Tables

The analysis is built on four core tables:
* ClickStream
* Product
* Customers
* Transactions

## Analysis Workflow
* Database & Schema Setup
* Data Ingestion
* Data Quality Checks
* Funnel Stage Definition
* Session-Level Funnel Construction
* Funnel Conversion Metrics
* Funnel Drop-Off Analysis
* Traffic Source Conversion Analysis
* Sessions that converted to an actual Transaction (Conversion Quality)
* Revenue Analysis by Traffic Source

## Key Analysis Performed

### Overall Funnel Performance
* Total sessions
* Page views to cart to purchase conversion
* End-to-end conversion rate

### Drop-off Analysis
* Cart → Purchase drop-offs

### Traffic Source Performance
* Conversions by traffic source
* Revenue analysis by traffic source

