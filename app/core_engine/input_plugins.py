"""
Input Plugins
=============
Original CSVReader and JSONReader logic preserved.
Removed CLI print statements; raise exceptions instead so
FastAPI can catch and return proper HTTP errors.
"""

from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
import json


class ReadData(ABC):
    def __init__(self, service):
        self.service = service

    @abstractmethod
    def run(self, file_path: str) -> None:
        pass


class CSVReader(ReadData):
    def run(self, file_path: str) -> None:
        df = pd.read_csv(file_path)
        df = df[df["Country Name"].notnull() & df["Continent"].notnull()]
        df = df.drop_duplicates(subset=["Country Name"], keep=False)

        yearCols = list(filter(lambda col: col.isdigit(), df.columns))
        df[yearCols] = df[yearCols].apply(
            lambda col: pd.to_numeric(
                col.astype(str).str.replace(r'[\$,]', '', regex=True),
                errors="coerce"
            ).fillna(0)
        )

        cleanRecords = df.to_dict(orient="records")
        self.service.execute(cleanRecords)


class JSONReader(ReadData):
    def run(self, file_path: str) -> None:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        content = content.replace('#@$!\\', '0')
        raw_data = json.loads(content)

        if not isinstance(raw_data, list):
            raw_data = [raw_data]

        filtered_records = list(filter(
            lambda x: x.get("Country Name") and x.get("Continent"), raw_data
        ))

        seen = set()
        deduplicated_records = []
        for record in filtered_records:
            country = record.get("Country Name")
            if country not in seen:
                seen.add(country)
                deduplicated_records.append(record)

        year_cols = list(filter(str.isdigit, deduplicated_records[0].keys())) if deduplicated_records else []

        for record in deduplicated_records:
            for year_col in year_cols:
                value = record.get(year_col)
                if value is None or value != value:
                    record[year_col] = 0
                else:
                    value_str = str(value).replace('$', '').replace(',', '')
                    try:
                        record[year_col] = float(value_str)
                    except ValueError:
                        record[year_col] = 0

        self.service.execute(deduplicated_records)
