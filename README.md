# NL OpenData MCP Server

A comprehensive **Model Context Protocol (MCP)** server for accessing Dutch government open data from [CBS (Centraal Bureau voor de Statistiek)](https://www.cbs.nl/) and [data.overheid.nl](https://data.overheid.nl/).

This is a small side project to experiment with MCP and see if it can be useful for accessing Dutch government open data. **It is not production ready and should not be used for production.**

I have tested it with Claude Desktop and LM Studio and it works wreasonably well. It should work with the most MCP clients.
The results are always dependent on the quality of the model you use. Frontier models understand data and mcp tools well and produce good results. Local models may not understand the tools calling and produce unexpected results.

For local models I got the best results using the Devstral2 small model with gpt-oss 20b close second. Context length is a very limiting factor for local models. Yo will need at least 30k tokens context length for basic analysis to work.

---

## ✨ Features

- **📊 Access 4800+ CBS Datasets** - Browse and query statistical data on population, economy, health, environment, and more
- **🔍 Smart Search** - Search datasets by title, summary, or both with local caching for fast results
- **📥 Flexible Data Fetching** - Query, filter, and download datasets in CSV or Parquet format
- **🦆 DuckDB Integration** - Save datasets directly to DuckDB for efficient SQL querying
- **🐍 Python Analysis** - Execute Pandas code directly on datasets without downloading
- **🗂️ Dual Source Support** - Check availability across CBS OData and data.overheid.nl

---

## 📦 Installation

### Using uvx (Recommended)

```bash
# Run directly from PyPI
uvx nl-opendata-mcp

# Or run directly from GitHub
uvx --from git+https://github.com/soulnai/nl-opendata-mcp.git nl-opendata-mcp
```

### Install with pip/uv

```bash
# Install from PyPI
uv pip install nl-opendata-mcp

# Install from GitHub
uv pip install git+https://github.com/soulnai/nl-opendata-mcp.git
```

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/soulnai/nl-opendata-mcp.git
cd nl-opendata-mcp

# Install dependencies and package in editable mode
uv sync
uv pip install -e .

# Run the server
uv run nl-opendata-mcp
```

---

## 🔧 Configuration

### Claude Desktop

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "nl-opendata-mcp": {
      "command": "uvx",
      "args": ["nl-opendata-mcp"]
    }
  }
}
```

### LM Studio

Add to your LM Studio configuration (`mcp.json`):

```json
{
  "mcpServers": {
    "nl-opendata-mcp": {
      "command": "uvx",
      "args": ["nl-opendata-mcp"]
    }
  }
}
```

### With Environment Variables

The server supports different transport modes:

```bash
# Default: stdio transport
uvx nl-opendata-mcp

# HTTP transport (port 8000)
TRANSPORT=http uvx nl-opendata-mcp

# SSE transport (port 8000)
TRANSPORT=sse uvx nl-opendata-mcp
```

---

## 🛠️ Available Tools

### Discovery Tools

| Tool | Description |
|------|-------------|
| `cbs_list_datasets` | List available datasets from the CBS catalog |
| `cbs_search_datasets` | Search datasets by keyword (in title, summary, or both) |
| `cbs_check_dataset_availability` | Check if a dataset is available via CBS OData or data.overheid.nl |
| `cbs_estimate_dataset_size` | Estimate dataset size before fetching (rows, columns, recommended strategy) |
| `cbs_inspect_dataset_details` | Get comprehensive dataset summary (metadata, structure, sample data) |

### Metadata Tools

| Tool | Description |
|------|-------------|
| `cbs_get_metadata` | Unified metadata tool - get info, structure, dimension values, or custom endpoints |

**`cbs_get_metadata` types:**
- `metadata_type="info"` - Dataset description (TableInfos)
- `metadata_type="structure"` - Column definitions and data types (DataProperties)
- `metadata_type="endpoints"` - Available metadata endpoints
- `metadata_type="dimensions"` - Dimension values with codes for filtering (requires `endpoint_name`)
- `metadata_type="custom"` - Query custom endpoint (requires `endpoint_name`)

### Data Fetching Tools

| Tool | Description |
|------|-------------|
| `cbs_query_dataset` | Query data with filtering and column selection |
| `cbs_save_dataset` | Save dataset to CSV (use `fetch_all=True` for complete dataset) |
| `cbs_save_dataset_to_duckdb` | Save dataset to DuckDB for SQL queries |

### Analysis Tools (disabled by default)

| Tool | Description |
|------|-------------|
| `cbs_analyze_remote_dataset` | Execute Python/Pandas code on a remote dataset |
| `cbs_analyze_local_dataset` | Execute Python/Pandas code on a local CSV file |

---

## 📖 Usage Examples

### 1. Discovering Datasets

**List available datasets:**
```
Use cbs_list_datasets with top=20 to see the first 20 datasets
```

**Search for population data:**
```
Use cbs_search_datasets with query="bevolking" to find population datasets
```

**Search only in titles:**
```
Use cbs_search_datasets with query="inflatie" and search_field="title"
```

---

### 2. Exploring a Dataset

**Get a comprehensive overview (recommended first step):**
```
Use cbs_inspect_dataset_details with dataset_id="85313NED"

# Returns:
# - Source confirmation (CBS OData or data.overheid.nl)
# - Title and description
# - Column definitions with types
# - Sample data (first 5 rows)
```

**Check dataset size before fetching:**
```
Use cbs_estimate_dataset_size with dataset_id="85313NED"

# Returns:
# - Estimated row count
# - Column count
# - Recommended fetch strategy
```

**Get detailed column structure:**
```
Use cbs_get_metadata with dataset_id="85313NED" and metadata_type="structure"

# Returns CSV with: Key, Type, Title, Description for each column
```

---

### 3. Querying Data

**Basic query with pagination:**
```
Use cbs_query_dataset with:
  - dataset_id="85313NED"
  - top=100
  - skip=0
```

**Query with OData filter:**
```
Use cbs_query_dataset with:
  - dataset_id="85313NED"
  - filter="Perioden eq '2023JJ00'"
  - top=50
```

**Select specific columns:**
```
Use cbs_query_dataset with:
  - dataset_id="85313NED"
  - select=["Perioden", "TotaleBevolking_1", "Mannen_2", "Vrouwen_3"]
  - top=100
```

**Query with multiple conditions:**
```
Use cbs_query_dataset with:
  - dataset_id="85313NED"
  - filter="Perioden eq '2023JJ00' and Leeftijd eq '10000'"
```

---

### 4. Downloading Full Datasets

**Save complete dataset to CSV:**
```
Use cbs_save_dataset with:
  - dataset_id="85313NED"
  - file_name="population_data.csv"
  - fetch_all=true
```

**Save to DuckDB for SQL analysis (supports column selection):**
```
Use cbs_save_dataset_to_duckdb with:
  - dataset_id="85313NED"
  - table_name="population"
  - fetch_all=true

# Creates datasets.db with table 'population'
```

---

### 5. Analyzing Data

**Note: Analysis tools are disabled by default. To enable them, set `NL_OPENDATA_MCP_USE_PYTHON_ANALYSIS=true` in the environment. It is security risk to let the model write and execute Python code on a client machine. Recommended for advanced users only. It's better to let the model query and save the data to a file, and let your CLI LLM coding tool to analyse it by writing and executing scripts. If you are running it in a non CLI environment (like LM Studio) and still want to give it the ability to analyse data, you can enable it by setting `NL_OPENDATA_MCP_USE_PYTHON_ANALYSIS=true` in the environment.**

**Analyze remote dataset with Pandas:**
```
Use cbs_analyze_remote_dataset with:
  - dataset_id="85313NED"
  - analysis_code="print(df.describe())"
```

**Calculate statistics:**
```
Use cbs_analyze_remote_dataset with:
  - dataset_id="85313NED"
  - analysis_code="result = df['TotaleBevolking_1'].mean()"
```

**Filter and aggregate:**
```
Use cbs_analyze_remote_dataset with:
  - dataset_id="85313NED"
  - analysis_code="""
    filtered = df[df['Perioden'].str.contains('2023')]
    result = filtered.groupby('Leeftijd')['TotaleBevolking_1'].sum()
    print(result)
    """
```

**Analyze a local CSV file:**
```
Use cbs_analyze_local_dataset with:
  - dataset_path="downloads/85313NED_full.csv"
  - analysis_code="print(df.info())"
```

---

### 6. Working with Metadata

**Get classification codes (e.g., gender categories):**
```
Use cbs_get_metadata with:
  - dataset_id="85313NED"
  - metadata_type="dimensions"
  - endpoint_name="Geslacht"
```

**Get time period definitions:**
```
Use cbs_get_metadata with:
  - dataset_id="85313NED"
  - metadata_type="dimensions"
  - endpoint_name="Perioden"
```

---

## 🎯 Common Workflows

### Workflow 1: Quick Data Exploration

```plaintext
1. cbs_search_datasets(query="unemployment")              # Find relevant datasets
2. cbs_inspect_dataset_details(dataset_id="82809NED")     # Get overview
3. cbs_query_dataset(dataset_id="82809NED", top=10)       # Preview data
```

### Workflow 2: Full Dataset Analysis

```plaintext
1. cbs_estimate_dataset_size(dataset_id="85313NED")       # Check size
2. cbs_save_dataset_to_duckdb(dataset_id="85313NED")      # Save to DuckDB
3. Query using DuckDB CLI or cbs_analyze_local_dataset
```

### Workflow 3: Filtered Data Export

```plaintext
1. cbs_get_metadata(dataset_id="85313NED", metadata_type="structure")  # Get column names
2. cbs_get_metadata(dataset_id="85313NED", metadata_type="dimensions", endpoint_name="Perioden")  # Get period codes
3. cbs_query_dataset(dataset_id="85313NED", filter="Perioden eq '2023JJ00'", select=["..."])  # Query
4. cbs_save_dataset(dataset_id="85313NED", file_name="filtered_data.csv")  # Export
```

---

## 📁 Output Formats

| Format | Use Case |
|--------|----------|
| **CSV** | Universal compatibility, works with Excel, Pandas, etc. |
| **DuckDB** | SQL queries, joins across multiple datasets |

---

## 🔗 OData Filter Examples

The CBS OData API uses OData v3 syntax for filtering:

```plaintext
# Exact match
Perioden eq '2023JJ00'

# Substring match
substringof('Amsterdam', RegioS)

# Multiple conditions
Perioden eq '2023JJ00' and Leeftijd eq '10000'

# OR conditions
Geslacht eq '1100' or Geslacht eq '2000'

# Numeric comparisons
TotaleBevolking_1 gt 100000
```

---

## 📚 Resources

- [CBS Open Data Portal](https://opendata.cbs.nl/)
- [CBS OData Documentation](https://www.cbs.nl/en-gb/our-services/open-data/statline-as-open-data)
- [data.overheid.nl](https://data.overheid.nl/)
- [MCP Protocol Specification](https://modelcontextprotocol.io/)

---
