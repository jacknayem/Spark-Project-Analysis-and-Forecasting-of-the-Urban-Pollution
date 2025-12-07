# Spark Project: Exploring and Forecasting Urban Pollution

This project analyzes and forecasts urban air pollution using Apache Spark. It follows a full data-processing pipeline — from ingestion and cleaning to exploratory analysis, modeling, and prediction. The goal is to understand pollution behavior across urban monitoring stations and build scalable forecasting tools.

---

## 🔧 Technologies Used
- Apache Spark  
- Scala  
- sbt  
- CSV pollution dataset (OpenAQ or similar)

---

## 📁 Project Structure
```
Pollution-Urbaine/
├── build.sbt ← sbt configuration (dependencies)
├── src/
│ ├── main/
│ │ ├── scala/
│ │ │ ├── ingestion/ ← Data ingestion & cleaning
│ │ │ │ └── DataIngestion.scala
│ │ │ ├── transformation/ ← Data cleaning & feature engineering
│ │ │ │ └── DataTransformation.scala
│ │ │ ├── analysis/ ← Exploratory data analysis
│ │ │ │ └── DataAnalysis.scala
│ │ │ ├── modeling/ ← Modeling relationships between stations
│ │ │ │ └── RelationshipModel.scala
│ │ │ ├── prediction/ ← Pollution forecasting
│ │ │ │ └── Prediction.scala
│ │ │ ├── realtime/ ← (Optional) real-time processing
│ │ │ │ └── RealTimeProcessing.scala
│ │ │ └── MainApp.scala ← Main entry point / pipeline controller
│ │ └── resources/ ← raw data files (e.g. CSV)
│ └── test/ ← (Optional) unit tests
```
---

## 🧩 Pipeline Overview

### **1. Data Ingestion**
- Load CSV pollution dataset  
- Infer schema  
- Remove empty or corrupted rows  
- Clean text fields (encoding, lowercase, trim)

### **2. Data Transformation**
- Remove duplicates  
- Filter out invalid or extreme pollutant values  
- Add new features:
  - hour, day, month, year
  - weekly/seasonal indicators
  - rolling statistics
  - station-wise pollutant summary metrics

### **3. Exploratory Data Analysis**
- Summary stats of each pollutant  
- Daily/seasonal pollution trends  
- Station-level pollution comparison  
- Identify data anomalies and variance patterns  

### **4. Modeling Station Relationships**
- Correlation between monitoring stations  
- Identify similar pollution patterns  
- Group stations with shared temporal behavior  

### **5. Forecasting Pollution**
- Predict future pollutant values  
- Short-term forecasting for each station  
- Generate prediction output table  

### **6. Optional Real-Time Processing**
- Real-time ingestion pipeline  
- Continuous prediction updates  

---

## ▶️ How to Run the Project

1. Install Spark, Scala, and sbt  
   - Setup guide:  
     [*Setting Up Apache Spark on Windows — Every Confusion, Fix, and Breakthrough I Had*](https://medium.com/@jacknayem/setting-up-apache-spark-on-windows-every-confusion-fix-and-breakthrough-i-had-2e3976fe5acb)

2. Clone the repository  

3. Place your dataset inside:


## 📤 Output

- Cleaned dataset  
- Pollution statistics  
- Station correlation results  
- Forecast values for selected pollutants  
- (Optional) real-time streaming outputs  

---

## 🚀 Future Improvements
- Add weather/traffic features  
- Improve forecasting accuracy  
- Deploy as API or dashboard  
- Add train/test split and evaluation metrics  

---

## 👤 Author
Project by **Abu Nayem**  
Medium article: [*Spark Project: Exploring and Forecasting Urban Pollution*](https://medium.com/@jacknayem/spark-project-exploring-and-forecasting-urban-pollution-85e5919c668c)



