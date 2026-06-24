# Big Data Analysis of Social Deprivation and Avoidable Mortality

## Overview

This project investigates the relationship between **social deprivation** and **avoidable mortality rates in England** using a Big Data analytics pipeline built with Hadoop, Apache Spark, and Apache Zeppelin.

The study combines deprivation and mortality datasets to identify whether socioeconomic inequalities are associated with higher rates of preventable deaths. By leveraging distributed storage and processing technologies, the project demonstrates how Big Data tools can be used to uncover meaningful public health insights.

---

## Project Objective

To determine:

> **Is there a significant relationship between social deprivation and avoidable mortality in England?**

The project analyzes deprivation scores and mortality rates across deprivation deciles and evaluates their correlation using large-scale data processing techniques.

---

## Key Findings

* Strong positive correlation between deprivation and avoidable mortality (**r ≈ 0.998**)
* Mortality rate in the most deprived areas was approximately **3.8 times higher** than in the least deprived areas
* High-deprivation communities experienced significantly greater avoidable mortality burdens
* Findings highlight substantial health inequalities across England

---

## Dataset Information

### 1. English Indices of Deprivation (IMD 2019)

Source: UK Government

**Dataset Size**

* 32,844 records

**Key Attributes**

* IMD Score
* IMD Decile
* Income Deprivation
* Health Deprivation
* Education Deprivation

### 2. Avoidable Mortality Dataset (ONS 2020)

Source: Office for National Statistics (ONS)

**Dataset Size**

* 32,844 records

**Key Attributes**

* Mortality Rate
* Deprivation Decile
* Avoidable Death Statistics
* Geographic Indicators

---

## Technology Stack

### Big Data Storage

* Hadoop Distributed File System (HDFS)

### Data Processing

* Apache Spark
* PySpark

### Data Analysis

* Python
* Spark SQL

### Data Visualization

* Apache Zeppelin

### Environment

* Docker

---

## System Architecture

```text id="2k8vpf"
Raw Datasets
      │
      ▼
 Hadoop HDFS Storage
      │
      ▼
 Apache Spark Processing
      │
      ├── Data Cleaning
      ├── Aggregation
      ├── Transformation
      └── Correlation Analysis
      │
      ▼
 Processed Dataset
 (deprivation_mortality.csv)
      │
      ▼
 Apache Zeppelin
      │
      ▼
 Visualizations & Insights
```

---

## Data Processing Pipeline

### Step 1: Data Ingestion

* Imported datasets into Docker environment
* Loaded datasets into Hadoop HDFS
* Verified distributed storage availability

### Step 2: Data Cleaning

#### IMD Dataset

* Selected deprivation score and decile columns
* Removed unnecessary attributes
* Generated cleaned dataset

#### Mortality Dataset

* Filtered records for:

  * Year 2020
  * Avoidable Mortality category
* Generated cleaned mortality dataset

### Step 3: Data Aggregation

Calculated:

* Mean IMD Score by Decile
* Average Mortality Rate by Decile

### Step 4: Data Integration

Joined datasets using:

```text id="q0u9b6"
IMD_Decile
```

Generated:

```text id="5o0xy0"
deprivation_mortality.csv
```

### Step 5: Correlation Analysis

Computed Pearson Correlation:

```text id="ikbhmv"
Correlation = 0.99812
```

---

## Analysis Performed

### Correlation Analysis

Examined the relationship between:

* Social Deprivation Score
* Avoidable Mortality Rate

Result:

* Extremely strong positive relationship
* Higher deprivation associated with higher mortality

### Mortality Gap Analysis

Compared:

* Most Deprived Areas
* Least Deprived Areas

Result:

```text id="8k1a8i"
Most Deprived: 522.45 deaths per 100,000
Least Deprived: 139 deaths per 100,000
Difference: 3.8x
```

### Deprivation Category Analysis

Grouped areas into:

* High Deprivation
* Medium Deprivation
* Low Deprivation

Compared mortality burden across categories.

### Cumulative Burden Analysis

Measured how avoidable mortality accumulates across deprivation deciles.

---

## Visualizations

The project includes multiple visual analytics generated in Apache Zeppelin:

* Scatter Plot (Deprivation vs Mortality)
* Mortality by Decile Bar Charts
* Pie Charts by Deprivation Category
* Stacked Comparison Charts
* Cumulative Mortality Analysis

These visualizations clearly demonstrate the relationship between deprivation and mortality.

---

## Project Structure

```text id="ll1e4y"
BigData-Deprivation-Mortality/
│
├── data/
│   ├── raw/
│   │   ├── imd2019.csv
│   │   └── ons2020.csv
│   │
│   ├── processed/
│   │   ├── imd_cleaned.csv
│   │   ├── ons2020_cleaned.csv
│   │   └── deprivation_mortality.csv
│
├── scripts/
│   ├── clean_imd.py
│   ├── clean_ons.py
│   └── analyze_deprivation_mortality.py
│
├── notebooks/
│   └── zeppelin_analysis.zpln
│
├── visualizations/
│
├── README.md
└── requirements.txt
```

---

## Running the Project

### Start Hadoop Environment

```bash id="97e6r4"
docker start sandbox-hdp
```

### Upload Data to HDFS

```bash id="cbh5ot"
hdfs dfs -put data/raw/* /user/project/data/raw/
```

### Run Spark Processing

```bash id="t0u5w6"
spark-submit scripts/analyze_deprivation_mortality.py
```

### Open Zeppelin

```bash id="pjr90n"
http://localhost:8080
```

---

## Results

| Metric                  | Value   |
| ----------------------- | ------- |
| Correlation Coefficient | 0.99812 |
| Highest Mortality Rate  | 522.45  |
| Lowest Mortality Rate   | 139.00  |
| Mortality Difference    | 3.8×    |
| Dataset Records         | 32,844  |

---

## Limitations

* Analysis restricted to 2020 mortality data
* Data aggregated to decile level
* Loss of Local Super Output Area (LSOA) detail
* Limited chart customization in Zeppelin

---

## Future Improvements

* Include multi-year mortality datasets
* Perform time-series trend analysis
* Integrate regional mapping visualizations
* Use Tableau or Power BI for interactive dashboards
* Apply machine learning models for mortality prediction
* Analyze additional socioeconomic indicators

---

## Learning Outcomes

This project demonstrates practical experience with:

* Big Data Analytics
* Hadoop Ecosystem
* Apache Spark
* Distributed Data Processing
* Data Engineering Pipelines
* Correlation Analysis
* Public Health Data Analysis
* Data Visualization
* Python and PySpark

---

## Author

**Bipin Shrestha**

MSc Computer Science and Technology
Ulster University

---

## License

This project is intended for educational and research purposes.
