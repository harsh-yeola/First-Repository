# RCSA DP2 Documentation

## Overview
The `rcsa_dp2_PROD_1.py` script processes Risk Control Self-Assessment (RCSA) data to generate alerts based on predefined rules and criteria. It connects to a database or reads data from local files, cleans and preprocesses the data, parses rule criteria, applies filtering logic, and saves the results to CSV files.

---

## Workflow

### 1. **Setup and Initialization**
- **Purpose**: Set up file paths, database connection parameters, and initialize variables.
- **Key Variables**:
  - `jdbcurl`: JDBC connection string for the database.
  - `classpath`: Path to the JDBC driver.
  - `current_file_directory`: Directory of the current script.

---

### 2. **Database Connection**
- **Purpose**: Establish a connection to the database using `jaydebeapi`.
- **Fallback**: If the connection fails, the script reads data from a local Excel file.

---

### 3. **Data Loading and Preprocessing**
- **Purpose**: Load RCSA data from the database or local file and preprocess it.
- **Steps**:
  - Rename columns to lowercase and replace spaces with underscores.
  - Replace missing values with `'NA'`.
  - Convert specific columns to appropriate data types (e.g., float, datetime).

---

### 4. **Rule Data and Criteria Loading**
- **Purpose**: Read rule data and criteria from CSV files.
- **Steps**:
  - Rename columns for consistency.
  - Prepare the data for parsing and filtering.

---

### 5. **Rule Parsing**
- **Purpose**: Parse rule criteria to generate logical conditions for filtering.
- **Steps**:
  - Map logical and mathematical operators to Python syntax.
  - Group rules by `rule_id` and `operator_priority` to build complex conditions.
  - Merge parsed rules with rule data.

---

### 6. **Filtering Logic**
- **Purpose**: Apply filtering logic to generate alerts based on rules.
- **Steps**:
  - Filter RCSA data using conditions derived from rule criteria.
  - Add metadata (e.g., rule name, description, alert status) to the filtered data.

---

### 7. **Alert Generation**
- **Purpose**: Combine alerts generated for each rule into a single DataFrame.
- **Steps**:
  - Iterate through active rule IDs.
  - Apply filtering logic and concatenate results.

---

### 8. **Save Results**
- **Purpose**: Save the execution status and generated alerts to CSV files.
- **Steps**:
  - Create a folder named with the current date.
  - Save execution status and alerts in separate files.

---

## Functions

### `comparison_rule_parser(df_rule_criteria)`
- **Purpose**: Parse rule criteria to generate logical conditions.
- **Inputs**:
  - `df_rule_criteria`: DataFrame containing rule criteria.
- **Outputs**:
  - DataFrame with `rule_id` and corresponding conditions.

---

### `get_categories(rule_id, column_name)`
- **Purpose**: Retrieve categories associated with a rule ID.
- **Inputs**:
  - `rule_id`: Rule identifier.
  - `column_name`: Column name to extract categories.
- **Outputs**:
  - List of categories.

---

### `filter_logic(df_rcsa_landingpage, df_rule_data, rule_id)`
- **Purpose**: Filter RCSA data based on rule logic.
- **Inputs**:
  - `df_rcsa_landingpage`: RCSA landing page data.
  - `df_rule_data`: Rule data.
  - `rule_id`: Rule identifier.
- **Outputs**:
  - Filtered DataFrame with alerts.

---

## Inputs
1. **Database Connection**: JDBC connection string and credentials.
2. **RCSA Data**: DataFrame loaded from the database or local file.
3. **Rule Data**: CSV file containing rule definitions.
4. **Rule Criteria**: CSV file containing rule criteria.

---

## Outputs
1. **Alerts Data**: CSV file containing generated alerts.
2. **Execution Status**: CSV file containing rule execution status.

---

## Key Notes
- **Error Handling**: The script gracefully handles database connection failures by falling back to local files.
- **Performance**: The script processes rules iteratively, which may impact performance for large datasets.
- **Modularity**: Functions are used to encapsulate specific tasks, improving readability and maintainability.

---

## Execution Time
The script prints the total execution time at the end:
```python
print(f'It took {(datetime.datetime.now() - start_time).seconds} seconds to finish the DP2 script run')
