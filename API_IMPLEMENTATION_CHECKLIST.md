# UK Energy Grid Tracker - API Implementation Checklist

## **Phase 1: Database Layer** 📊

### **Core Aggregation Functions**
- [ ] Add `get_generation_aggregated()` to `database.py`
  - [ ] Handle datetime range parameters
  - [ ] Implement granularity calculation (30min, 1hr, 2hr, 4hr, 6hr, 12hr, 24hr)
  - [ ] Use SQLite `strftime()` for time binning
  - [ ] Calculate avg/high/low for each source in each time bin
  - [ ] **Handle missing data points** - skip empty bins or provide null values

### **Helper Functions**
- [ ] Add `_calculate_time_bins()` helper
- [ ] Add `_aggregate_source_data()` helper  
- [ ] Add `_validate_granularity()` helper
- [ ] Add `_parse_source_groups()` helper for grouping logic

### **SQL Query Optimization**
- [ ] Create efficient SQL aggregation queries
- [ ] Add database indexes if needed for performance
- [ ] Test with large date ranges for performance

---

## **Phase 2: API Layer** 🌐

### **Dependencies & Setup**
- [ ] Add FastAPI to `requirements.txt`
- [ ] Create `api.py` with FastAPI application
- [ ] Set up basic routing structure

### **Core Endpoint Implementation**
- [ ] Implement `GET /api/generation/aggregated` endpoint
- [ ] Add parameter validation and parsing
  - [ ] Validate datetime formats
  - [ ] Validate granularity values
  - [ ] Validate source names
  - [ ] Parse JSON groups parameter
- [ ] **Handle missing data gracefully** in API responses
- [ ] Format response data structure
- [ ] Add comprehensive error handling

### **Response Formatting**
- [ ] Implement metadata section (start_time, end_time, granularity, time_bins)
- [ ] Implement data section with sources and groups
- [ ] Handle empty results appropriately

---

## **Phase 3: Integration** 🔗

### **Docker Architecture Decision**
**Recommendation: Same Docker process** - Here's why:
- **Simpler deployment** - single container to manage
- **Shared database access** - no need for container networking
- **Resource efficiency** - one process instead of two
- **Easier debugging** - logs in one place
- **FastAPI is lightweight** - minimal overhead

### **Main Application Integration**
- [ ] Update `main.py` to include FastAPI server
- [ ] Run API server alongside data collection loop
- [ ] Add API health check to existing health monitoring
- [ ] Update Dockerfile if needed

### **Configuration Updates**
- [ ] Add API settings to `config.py`
  - [ ] API port and host settings
  - [ ] CORS configuration
  - [ ] Rate limiting settings (future)
- [ ] Update `docker-compose.yml` if needed

---

## **Phase 4: Testing & Documentation** 🧪

### **Unit Testing**
- [ ] Test database aggregation functions
  - [ ] Test with various granularities
  - [ ] Test with missing data scenarios
  - [ ] Test grouping functionality
- [ ] Test API endpoint validation
- [ ] Test error handling paths

### **Integration Testing**
- [ ] Test full API request/response cycle
- [ ] Test with real database data
- [ ] Test edge cases (empty ranges, invalid parameters)

### **API Documentation**
- [ ] Add OpenAPI/Swagger documentation
- [ ] Create example requests/responses
- [ ] Document error codes and messages

---

## **Missing Data Handling Strategy** ⚠️

### **Database Level**
- [ ] **Skip empty bins** - Return null/None for sources with no data
- [ ] **Document data quality** - Include count of data points in each bin
- [ ] **Graceful degradation** - Still return results for partial data

### **API Level**
- [ ] **Clear null indicators** - Use explicit null values in JSON
- [ ] **Metadata about data quality** - Include data point counts in response
- [ ] **Informative error messages** - Explain when data is missing

### **Example Response with Missing Data:**
```json
{
  "metadata": {
    "data_quality": {
      "total_expected_bins": 24,
      "bins_with_data": 22,
      "missing_bins": 2
    }
  },
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "sources": {
        "solar": {"avg": 2.5, "high": 3.1, "low": 0.0, "data_points": 2},
        "wind": {"avg": null, "high": null, "low": null, "data_points": 0}
      }
    }
  ]
}
```

---

## **Implementation Priority** 🎯

1. **Start with database functions** - Core aggregation logic
2. **Add basic API endpoint** - Simple GET with validation
3. **Implement missing data handling** - Robust data quality management
4. **Add grouping functionality** - Dynamic source combinations
5. **Integration and testing** - Full system validation

---

## **API Endpoint Specification**

### **Endpoint:** `GET /api/generation/aggregated`

### **Query Parameters:**
- `start_time`: ISO 8601 timestamp
- `end_time`: ISO 8601 timestamp  
- `granularity_minutes`: 30, 60, 120, 240, 360, 720, 1440 (default: 30)
- `sources`: Comma-separated list of energy sources
- `groups`: JSON string defining source groupings

### **Example Request:**
```
GET /api/generation/aggregated?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&granularity_minutes=60&sources=solar,wind,nuclear,biomass&groups={"low_carbon":["solar","wind","nuclear"],"renewable":["solar","wind","biomass"]}
```

### **Response Format:**
```json
{
  "metadata": {
    "start_time": "2024-01-01T00:00:00Z",
    "end_time": "2024-01-02T00:00:00Z", 
    "granularity_minutes": 60,
    "time_bins": 24
  },
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00Z",
      "sources": {
        "solar": {"avg": 2.5, "high": 3.1, "low": 0.0},
        "wind": {"avg": 8.2, "high": 12.3, "low": 4.1},
        "nuclear": {"avg": 15.7, "high": 16.2, "low": 15.1},
        "biomass": {"avg": 3.8, "high": 4.2, "low": 3.5}
      },
      "groups": {
        "low_carbon": {"avg": 26.4, "high": 31.6, "low": 19.2},
        "renewable": {"avg": 14.5, "high": 19.6, "low": 7.6}
      }
    }
  ]
}
``` 