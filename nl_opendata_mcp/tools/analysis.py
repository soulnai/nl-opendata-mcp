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
                'size_kb': round(size_kb, 1),
                'rows': _count_csv_rows(full_path)
            })

    if not files:
        return "No CSV files found in downloads directory. Use cbs_save_dataset first to download data."

    output = ["AVAILABLE LOCAL DATASETS", "=" * 40]
    output.append(f"Directory: {os.path.abspath(downloads_path)}")
    output.append("")

    for f in sorted(files, key=lambda x: x['filename']):
        output.append(f"- {f['filename']} ({f['size_kb']} KB, ~{f['rows']} rows)")

    output.append("")
    output.append("Use cbs_analyze_local_dataset with dataset_name='<filename>' to analyze.")

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

    Fetches the first 1000 records from the dataset and executes your analysis code.
    The data is available as a pandas DataFrame named 'df'.

    Args:
        params: AnalyzeRemoteInput containing:
            - dataset_id (str): CBS Dataset ID (e.g., '85313NED')
            - analysis_code (str): Python code to execute. Use print() for output.
            - script_path (str, optional): Path to .py file (alternative to analysis_code)

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
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json"
    ctx.info(f"Fetching data for analysis: {url}")
    logger.info(f"Analyzing remote dataset: {dataset_id}")

    try:
        response = await fetch_with_retry(url)
        records = response.json().get('value', [])

        if not records:
            return "No data found in dataset."

        df = pd.DataFrame(records)

        # Auto-translate coded dimension values to human-readable text
        try:
            ctx.info(f"Translating dimension values for {dataset_id}...")
            df = await translator.translate_dataframe(df, dataset_id)
        except Exception as e:
            logger.warning(f"Translation failed for {dataset_id}: {e}")

        local_env = {'df': df, 'pd': pd, 'np': np}

        ctx.info(f"Loaded {len(df)} rows, {len(df.columns)} columns. Executing analysis code...")

        code_to_exec = params.analysis_code
        if params.script_path:
            try:
                with open(params.script_path, 'r') as f:
                    code_to_exec = f.read()
            except Exception as e:
                return f"Error reading script file: {e}"

        if not code_to_exec:
            return "Error: Neither analysis_code nor script_path provided."

        try:
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            redirected_output = sys.stdout = StringIO()

            exec(code_to_exec, {}, local_env)

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

    except Exception as e:
        return handle_http_error(e, "cbs_analyze_remote_dataset")


async def cbs_analyze_local_dataset(ctx: Context, params: AnalyzeLocalInput) -> str:
    """
    Analyzes a local CSV dataset using Python/Pandas code. Can create charts and save to files.

    IMPORTANT: First use cbs_list_local_datasets to see available files!

    Args:
        params: AnalyzeLocalInput containing:
            - dataset_name (str): Filename in downloads folder (e.g., 'population.csv')
            - analysis_code (str): Python code to execute. Use print() for output.
            - script_path (str, optional): Path to .py file (alternative to analysis_code)

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

    Workflow:
        1. First call cbs_list_local_datasets to see available files
        2. Then call this tool with the exact filename
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
        if params.script_path:
            try:
                with open(params.script_path, 'r') as f:
                    code_to_exec = f.read()
            except Exception as e:
                return f"Error reading script file: {e}"

        if not code_to_exec:
            return "Error: Neither analysis_code nor script_path provided."

        try:
            import sys
            from io import StringIO
            old_stdout = sys.stdout
            redirected_output = sys.stdout = StringIO()

            exec(code_to_exec, {}, local_env)

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
