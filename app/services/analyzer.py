"""
GDPAnalyzerService
==================
Thin service wrapper that:
1. Accepts an uploaded file path + JSON config dict
2. Converts Excel → CSV if needed (same as original ConvertExcelToCSV)
3. Runs the original pipeline (CSVReader → TransformationEngine → MemorySink)
4. Returns structured results + metadata

Integration point: routes call this class; core_engine stays untouched.
"""

import logging
import os
import json
import pandas as pd
from typing import Dict, Any, List

from app.core_engine.engine import TransformationEngine, MemorySink
from app.core_engine.input_plugins import CSVReader, JSONReader

logger = logging.getLogger(__name__)

# Maps FunctionOption int → human-readable name
FUNCTION_NAMES = {
    1: "Top 10 Countries by GDP",
    2: "Bottom 10 Countries by GDP",
    3: "GDP Growth Rate by Country",
    4: "Average GDP by Continent",
    5: "Global GDP Trends",
    6: "Fastest Growing Continent",
    7: "Countries with Consistent GDP Decline",
    8: "Global GDP Contribution by Continent",
}


class GDPAnalyzerService:

    def load_file(self, file_path: str) -> str:
        """
        Returns file_path unchanged; validates the file exists and is readable.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Uploaded file not found: {file_path}")
        return file_path

    def convert_to_csv(self, excel_path: str, csv_path: str) -> str:
        """
        Mirrors original ConvertExcelToCSV().
        Converts .xlsx → .csv and returns the csv path.
        """
        logger.info(f"Converting Excel to CSV: {excel_path} → {csv_path}")
        df = pd.read_excel(excel_path)
        df.to_csv(csv_path, index=False)
        return csv_path

    def parse_data(self, file_path: str, input_type: str, engine: TransformationEngine):
        """
        Selects the correct reader and runs it (mirrors bootstrap() reader logic).
        """
        readers = {
            "csv": CSVReader,
            "json": JSONReader,
        }
        reader_class = readers.get(input_type)
        if not reader_class:
            raise ValueError(f"Unknown input_provider: {input_type}. Use 'csv' or 'json'.")

        reader = reader_class(service=engine)
        reader.run(file_path)

    def apply_config(self, config: dict) -> dict:
        """
        Validates required config keys and returns cleaned config.
        Raises ValueError with clear messages if config is invalid.
        """
        required = ["FunctionOption", "parameters"]
        for key in required:
            if key not in config:
                raise ValueError(f"Config missing required key: '{key}'")

        func_option = int(config.get("FunctionOption", 0))
        if func_option not in range(1, 9):
            raise ValueError(f"FunctionOption must be 1–8, got: {func_option}")

        params = config.get("parameters", {})
        year_range = params.get("YearRange", {})
        if "startYear" not in year_range or "EndYear" not in year_range:
            raise ValueError("Config.parameters.YearRange must contain 'startYear' and 'EndYear'")

        return config

    def compute_statistics(self, file_path: str, config: dict) -> List[dict]:
        """
        Main pipeline: load → parse → transform → return results list.
        """
        validated_config = self.apply_config(config)
        sink = MemorySink()
        engine = TransformationEngine(sink=sink, params=validated_config)
        input_type = config.get("input_provider", "csv")

        # If Excel file was uploaded, convert first
        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            csv_path = file_path.rsplit(".", 1)[0] + ".csv"
            file_path = self.convert_to_csv(file_path, csv_path)
            input_type = "csv"

        self.parse_data(file_path, input_type, engine)
        return sink.results

    def generate_output(self, results: List[dict], config: dict) -> dict:
        """
        Wraps raw results with metadata for the API response.
        """
        func_option = int(config.get("FunctionOption", 0))
        analysis_name = FUNCTION_NAMES.get(func_option, f"Analysis #{func_option}")

        # Infer chart config hints from original core.json chart section
        chart_config = config.get("chart", {})
        chart_params = chart_config.get("params", {}).get(str(func_option), {})

        return {
            "analysis_name": analysis_name,
            "function_option": func_option,
            "record_count": len(results),
            "chart_hint": {
                "type": chart_params.get("type"),#this
                "x_key": chart_params.get("x_key"),
                "y_key": chart_params.get("y_key"),
                "title": chart_params.get("title", analysis_name),
                "x_label": chart_params.get("x_label", ""),
                "y_label": chart_params.get("y_label", ""),
            },
            "results": results,
        }
