Here is a clear, **development roadmap** for your **Streamlit-based volume anomaly detection dashboard**, using **statistics and rule-based methods** (no ML). This roadmap balances robustness, modularity, and speed of iteration.

---

# 📍 Project: Volume Anomaly Detection Dashboard (Non-ML Stack)

### 🔧 Objective:

Build a multi-asset Streamlit dashboard that detects **unexpected spikes in hourly trading volume** by analyzing **Relative Volume (RVOL)** over 2 years of historical 1H data, while **filtering out expected spikes** due to macroeconomic events or market-wide volatility.

---

## 🧭 PHASE 1: Data Preparation and Infrastructure

### ✅ 1.1. Define Data Requirements

* [ ] 1H price and volume data for all target assets (2+ years).
* [ ] Daily/weekly macroeconomic calendar with timestamps (FOMC, CPI, NFP, etc.).
* [ ] Market-wide data: SPY volume, VIX, or sector ETFs (optional but useful).
* [ ] Timestamp alignment in UTC or a common timezone.

---

### ✅ 1.2. Build Data Loader Module

* [ ] Load multi-asset 1H volume/price data (CSV or API).
* [ ] Load macroeconomic calendar and flag timestamps.
* [ ] Load market-wide metrics and forward-fill to 1H intervals.
* [ ] Join datasets on timestamps.

**Output:** `DataFrame` with asset, timestamp, volume, price, event flag, VIX, SPY vol, etc.

---

### ✅ 1.3. Feature Engineering

For each asset:

* [ ] Compute 30-period EMA of volume (`ema_volume`).
* [ ] Compute `rvol = actual_volume / ema_volume`.
* [ ] Compute 24-hour rolling stats:

  * Rolling mean/std of volume and rvol
  * Rolling Z-score: `(rvol - mean) / std`
* [ ] Weekly aggregation of:

  * RVOL mean, std, max, skew

**Output:** Cleaned and enriched dataset with engineered features.

---

## 📊 PHASE 2: Statistical Anomaly Detection (Non-ML)

### ✅ 2.1. Time Series Decomposition

* [ ] Apply STL decomposition on 1H volume to extract:

  * `Trend`, `Seasonal`, `Residual`
* [ ] Flag anomalies using residuals:

  ```python
  anomaly = abs(residual) > 2 * residual.std()
  ```

---

### ✅ 2.2. Statistical Rules for Anomalies

* [ ] Rule 1: **Z-score**

  * `rvol_zscore > 2.5 → anomaly`
* [ ] Rule 2: **IQR method**

  * Outliers if RVOL outside Q1 - 1.5*IQR or Q3 + 1.5*IQR
* [ ] Rule 3: **No Event**

  * Only flag anomaly if `event_flag == False`

---

### ✅ 2.3. Weekly Anomaly Detection

* [ ] Aggregate weekly RVOL stats.
* [ ] Calculate Z-score of weekly `rvol_mean`.
* [ ] Flag weekly anomalies:

  ```python
  abs(weekly_z_score) > 2.5
  ```

---

## 🖥️ PHASE 3: Streamlit Dashboard

### ✅ 3.1. Dashboard Layout

#### 🧭 Sidebar Controls:

* [ ] Asset selector (multi-select)
* [ ] Date range picker
* [ ] Toggle event-filtered anomalies only
* [ ] Timeframe: Intraday or Weekly

---

### ✅ 3.2. Tabs or Sections

#### 📈 Intraday Anomalies

* Line chart of RVOL over time

  * Overlay macro event markers
  * Highlight anomaly timestamps (dots or color change)
* Histogram of RVOL (with anomaly thresholds)

#### 📅 Weekly Summary

* Weekly table:

  * Date, mean RVOL, std, z-score, anomaly flag
* Bar plot of weekly mean RVOL vs 2Y average

#### 🧩 Decomposition Viewer

* Trend, seasonality, residual plots (STL)
* Anomaly periods marked on residuals

#### 📋 Event Overlay Panel

* Show nearby macro events
* Flag overlapping anomalies

---

## 🧪 PHASE 4: Testing and Validation

### ✅ 4.1. Functional Testing

* [ ] Load multiple assets successfully
* [ ] Anomalies are only flagged on **non-event days**
* [ ] Weekly anomalies show consistent thresholds

### ✅ 4.2. Visual Validation

* [ ] Confirm volume spikes are accurately reflected
* [ ] Check if known event days are filtered out

---

## 🚀 PHASE 5: Optimization & Deployment

### ✅ 5.1. Performance

* [ ] Optimize pandas pipelines with `.loc`, `.query()`
* [ ] Cache decomposed series using `@st.cache_data`

### ✅ 5.2. Export/Share

* [ ] Allow download of anomaly logs (CSV)
* [ ] Streamlit Cloud or internal server deployment

---

## 🧠 Bonus: Future Enhancements (Post-v1)

* [ ] Add clustering of anomalies by time-of-day
* [ ] Allow user-defined anomaly thresholds
* [ ] Compare across sectors or correlated assets
* [ ] Alert system: send notification/email when anomaly is flagged

---

## 🗂️ Final File Structure Suggestion

```
project_root/
│
├── data/
│   ├── asset_data.csv
│   └── events.csv
│
├── modules/
│   ├── loader.py         # Data loading
│   ├── features.py       # Feature engineering
│   ├── rules.py          # Anomaly logic
│   ├── plots.py          # Streamlit visualizations
│
├── dashboard.py          # Main Streamlit app
└── requirements.txt
```

---

Would you like me to now scaffold the folder or write the first code module (`features.py`) for this system?
