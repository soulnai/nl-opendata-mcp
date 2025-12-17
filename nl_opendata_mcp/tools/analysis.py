"""
Analysis tools for executing Python/Pandas code on CBS datasets.
"""
import logging
import os
import pandas as pd
import numpy as np
from fastmcp import Context

from ..config import get_settings
from ..models import AnalyzeRemoteInput, AnalyzeLocalInput
from ..services.http_client import fetch_with_retry
from ..services.translator import translator
from ..utils import (
    handle_http_error,
    validate_dataset_id,
    safe_join_path,
    sanitize_odata_filter,
    sanitize_select_columns,
    ValidationError,
    MCPError,
)

logger = logging.getLogger(__name__)
settings = get_settings()


async def cbs_list_local_datasets(ctx: Context) -> str:
    """
    Lists all locally saved datasets in the downloads directory.

    Returns:
        str: List of available CSV files with their sizes and modification times.

    Use this tool BEFORE cbs_analyze_local_dataset to see what files are available.
    """
    downloads_path = settings.downloads_path

    if not os.path.exists(downloads_path):
        return f"Downloads directory does not exist: {downloads_path}"

    files = []
    for f in os.listdir(downloads_path):
        if f.endswith('.csv'):
            full_path = os.path.join(downloads_path, f)
            stat = os.stat(full_path)
            size_kb = stat.st_size / 1024
            files.append({
                'filename': f,
                'full_path': os.abspath(full_path),
                'size_kb': round(size_kb, 1),
                'rows': _count_csv_rows(full_path)
            })

    if not files:
        return "No CSV files found in downloads directory. Use cbs_save_dataset first to download data."

    output = []
    for f in sorted(files, key=lambda x: x['filename']):
        output.append(f"{f['full_path']} ({f['size_kb']} KB, ~{f['rows']} rows)")

    return "\n".join(output)


def _count_csv_rows(path: str) -> str:
    """Count rows in CSV file (approximate for large files)."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            # Count first 1000 lines to estimate
            count = sum(1 for _ in f) - 1  # -1 for header
            return str(count) if count < 1000 else f"{count}+"
    except:
        return "?"


async def cbs_analyze_remote_dataset(ctx: Context, params: AnalyzeRemoteInput) -> str:
    """
    Analyzes a remote CBS dataset using Python/Pandas code. Can create charts and save to files.

    Fetches data from the dataset (with optional filtering) and executes your analysis code.
    The data is available as a pandas DataFrame named 'df'.

    Args:
        params: AnalyzeRemoteInput containing:
            - dataset_id (str): CBS Dataset ID (e.g., '85313NED')
            - analysis_code (str): Python code to execute. Use print() for output.
            - script_path (str, optional): Path to .py file (alternative to analysis_code)
            - filter (str, optional): OData filter to apply BEFORE fetching data
            - select (List[str], optional): Column names to fetch (reduces data transfer)
            - top (int): Maximum records to fetch (default: 10000)
            - translate (bool): Translate coded values to human-readable text (default: True)

    IMPORTANT - Translation Behavior:
        When translate=True (default), dimension values are converted to human-readable text.
        - OData filter uses RAW codes: filter="Luchthavens eq 'A043591'"
        - DataFrame contains TRANSLATED values: df['Luchthavens'] == 'Eindhoven Airport'

        To find dimension codes, use cbs_get_dimension_values tool first.

    Available variables in your code:
        - df: pandas DataFrame with the dataset
        - pd: pandas module
        - np: numpy module

    Returns:
        str: Printed output from your code, or value of 'result' variable if set.

    IMPORTANT - Creating Charts:
        This tool CAN create and save charts/visualizations to files!
        Use matplotlib to create charts and save them with plt.savefig().
        The user can then view the saved image file.

        Chart Example:
        ```python
        import matplotlib.pyplot as plt

        # Create chart
        df.plot(kind='bar', x='RegioS', y='Value')
        plt.title('My Chart')
        plt.tight_layout()

        # Save to file (REQUIRED - don't use plt.show())
        plt.savefig('./output/my_chart.png', dpi=150)
        plt.close()
        print('Chart saved to ./output/my_chart.png')
        ```

    Code Examples:
        # Show basic statistics
        analysis_code="print(df.describe())"

        # Show column names and types
        analysis_code="print(df.dtypes)"

        # Count unique values in a column
        analysis_code="print(df['ColumnName'].value_counts())"

        # Filter and aggregate
        analysis_code=\"\"\"
        filtered = df[df['Year'] == 2023]
        print(filtered.groupby('Region').sum())
        \"\"\"

        # Assign to result variable (alternative to print)
        analysis_code="result = df.head(10).to_string()"
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
        sanitized_filter = sanitize_odata_filter(params.filter)
        sanitized_select = sanitize_select_columns(params.select)
    except ValidationError as e:
        return e.to_error_string()

    # Build URL with optional filter, select, and top parameters
    url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top={params.top}"
    if sanitized_filter:
        url += f"&$filter={sanitized_filter}"
    if sanitized_select:
        url += f"&$select={','.join(sanitized_select)}"

    ctx.info(f"Fetching data for analysis: {url}")
    logger.info(f"Analyzing remote dataset: {dataset_id}, top={params.top}, filter={sanitized_filter}, translate={params.translate}")

    try:
        response = await fetch_with_retry(url)
        records = response.json().get('value', [])

        if not records:
            # Provide helpful diagnostics when no data found
            diagnostic = [
                "No data found in dataset.",
                f"Dataset: {dataset_id}",
            ]
            if sanitized_filter:
                diagnostic.append(f"Filter applied: {sanitized_filter}")
                diagnostic.append("TIP: Check if filter values are correct. Use cbs_get_dimension_values to find valid codes.")
            return "\n".join(diagnostic)

        df = pd.DataFrame(records)

        # Auto-translate coded dimension values to human-readable text
        if params.translate:
            try:
                ctx.info(f"Translating dimension values for {dataset_id}...")
                df = await translator.translate_dataframe(df, dataset_id)
            except Exception as e:
                logger.warning(f"Translation failed for {dataset_id}: {e}")

        # Provide data preview info for debugging
        ctx.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        ctx.info(f"Columns: {', '.join(df.columns.tolist()[:8])}{'...' if len(df.columns) > 8 else ''}")
        if len(df) > 0:
            # Show sample values for first few columns to help with debugging
            sample_cols = df.columns.tolist()[:3]
            sample_vals = {col: df[col].iloc[0] for col in sample_cols}
            ctx.info(f"Sample values (row 0): {sample_vals}")

        local_env = {'df': df, 'pd': pd, 'np': np}

        code_to_exec = params.analysis_code
        if params.script_path:
            try:
                with open(params.script_path, 'r') as f:
                    code_to_exec = f.read()
            except Exception as e:
                return f"Error reading script file: {e}"

        if not code_to_exec:
            # If no code provided, return data summary
            summary = [
                f"DataFrame loaded: {len(df)} rows, {len(df.columns)} columns",
                f"Columns: {', '.join(df.columns.tolist())}",
                "",
                "Sample data (first 5 rows):",
                df.head().to_string(),
            ]
            return "\n".join(summary)

        try:
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            redirected_output = sys.stdout = StringIO()

            exec(code_to_exec, local_env)

            sys.stdout = old_stdout
            output = redirected_output.getvalue()

            if 'result' in local_env:
                return str(local_env['result'])
            elif output:
                return output
            else:
                return "Code executed successfully but produced no output. Please print() the result or assign to 'result'."

        except Exception as exec_err:
            # Enhanced error message with data context
            sys.stdout = old_stdout
            logger.error(f"Analysis code execution error: {exec_err}")
            error_context = [
                f"Error executing analysis code: {exec_err}",
                "",
                "DataFrame context for debugging:",
                f"  - Rows: {len(df)}",
                f"  - Columns: {', '.join(df.columns.tolist()[:10])}{'...' if len(df.columns) > 10 else ''}",
            ]
            if len(df) > 0:
                error_context.append(f"  - Sample row: {df.iloc[0].to_dict()}")
            return "\n".join(error_context)

    except Exception as e:
        return handle_http_error(e, "cbs_analyze_remote_dataset")


async def cbs_analyze_local_dataset(ctx: Context, params: AnalyzeLocalInput) -> str:
    """
    Analyzes a local CSV dataset using Python/Pandas code. Can create charts and save to files.

    Args:
        params: AnalyzeLocalInput containing:
            - dataset_name (str): Filename in downloads folder (e.g., 'population.csv')
            - analysis_code (str): Python code to execute. Use print() for output.

    Available variables in your code:
        - df: pandas DataFrame with the CSV data
        - pd: pandas module
        - np: numpy module

    Returns:
        str: Printed output from your code, or value of 'result' variable if set.

    IMPORTANT - Creating Charts:
        This tool CAN create and save charts/visualizations to files!
        Use matplotlib to create charts and save them with plt.savefig().
        The user can then view the saved image file.

        Chart Example:
        ```python
        # Create chart
        df.plot(kind='bar', x='RegioS', y='Value')
        plt.title('My Chart')
        plt.tight_layout()

        # Save to file (REQUIRED - don't use plt.show())
        plt.savefig('./output/my_chart.png', dpi=150)
        plt.close()
        print('Chart saved to ./output/my_chart.png')
        ```

    Code Examples:
        # Show first rows
        analysis_code="print(df.head())"

        # Show column info
        analysis_code="print(df.info())"

        # Basic statistics
        analysis_code="print(df.describe())"

        # Group and aggregate
        analysis_code=\"\"\"
        result = df.groupby('Category').agg({
            'Value': ['mean', 'sum', 'count']
        })
        print(result)
        \"\"\"
    """
    ctx.info(f"Loading data for analysis: {params.dataset_name}")
    logger.info(f"Analyzing local dataset: {params.dataset_name}")

    try:
        # Use safe path joining to prevent path traversal
        full_path = safe_join_path(settings.downloads_path, params.dataset_name)
    except (ValidationError, MCPError) as e:
        return e.to_error_string()

    try:
        df = pd.read_csv(full_path)

        if df.empty:
            return "No data found in dataset."

        local_env = {'df': df, 'pd': pd, 'np': np}

        ctx.info(f"Loaded {len(df)} rows, {len(df.columns)} columns. Executing analysis code...")

        code_to_exec = params.analysis_code

        if not code_to_exec:
            return "Error: analysis_code not provided."

        try:
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            redirected_output = sys.stdout = StringIO()

            exec(code_to_exec, local_env)

            sys.stdout = old_stdout
            output = redirected_output.getvalue()

            if 'result' in local_env:
                return str(local_env['result'])
            elif output:
                return output
            else:
                return "Code executed successfully but produced no output. Please print() the result or assign to 'result'."

        except Exception as exec_err:
            logger.error(f"Analysis code execution error: {exec_err}")
            return f"Error executing analysis code: {exec_err}"

    except FileNotFoundError:
        return f"Error: File not found: {params.dataset_name}"
    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        return f"Error loading dataset: {str(e)}"
