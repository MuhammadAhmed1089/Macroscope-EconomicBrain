"""
GDP Analyzer Core Engine
========================
This is the original TransformationEngine from the CLI project,
preserved exactly. Only change: wrapped in GDPAnalyzer service class
so it can be called from FastAPI routes without CLI/file dependencies.
"""

from functools import reduce
from typing import List, Any, Dict


# ── Original TransformationEngine (UNCHANGED) ──────────────────────────────

class TransformationEngine:
    def __init__(self, sink, params: dict):
        self.sink = sink
        self.params = params

    def execute(self, rawData: list):
        try:
            functionCall = {
                1: self.top10,
                2: self.bottom10,
                3: self.gdp_growth_rate,
                4: self.average_gdp_by_continent,
                5: self.GlobalGDPTrends,
                6: self.fastestGrowingContinent,
                7: self.countriesWithConsistentDecline,
                8: self.globalGdpContribution
            }

            user_choice = int(self.params.get("FunctionOption"))
            innerParams = self.params.get("parameters", {})
            yearRange = innerParams.get("YearRange", {})

            start = yearRange.get("startYear")
            end = yearRange.get("EndYear")

            if start is None or end is None:
                raise ValueError("startYear or EndYear missing from parameters.YearRange")

            funcParameters = {
                "continent": innerParams.get("continent"),
                "targetYear": innerParams.get("year"),
                "lastXYears": innerParams.get("lastXyears"),
                "continents": innerParams.get("continents", []),
                "startYear": start,
                "endYear": end
            }

            function = functionCall[user_choice]
            results = function(rawData, **funcParameters)

            self.sink.write(results)

        except Exception as e:
            raise RuntimeError(f"Execution Error: {e}") from e

    def top10(self, data, continent, targetYear, **kwargs):
        year = str(targetYear)
        filtered = list(filter(
            lambda r: r.get("Continent") == continent and year in r, data
        ))
        sorted_data = sorted(filtered, key=lambda r: float(r.get(year, 0) or 0), reverse=True)
        top10_list = sorted_data[:10]
        return list(map(
            lambda pair: {
                "rank": pair[0] + 1,
                "country": pair[1].get("Country Name"),
                "continent": continent,
                "year": year,
                "gdp": pair[1].get(year),
            },
            enumerate(top10_list)
        ))

    def bottom10(self, data, continent, targetYear, **kwargs):
        year = str(targetYear)
        filtered = list(filter(
            lambda r: r.get("Continent") == continent and year in r, data
        ))
        sorted_data = sorted(filtered, key=lambda r: float(r.get(year, 0) or 0))
        bottom10 = sorted_data[:10]
        return list(map(
            lambda pair: {
                "rank": pair[0] + 1,
                "country": pair[1].get("Country Name"),
                "continent": continent,
                "year": year,
                "gdp": pair[1].get(year),
            },
            enumerate(bottom10)
        ))

    def continentGdp(self, data, continent, startYear, endYear):
        filtered = list(filter(lambda r: r.get("Continent") == continent, data))

        def process_country(record):
            years_range = list(map(str, range(int(startYear), int(endYear) + 1)))
            return reduce(lambda acc, yr: acc + float(record.get(yr, 0) or 0), years_range, 0.0)

        result = list(map(process_country, filtered))
        return reduce(lambda acc, x: x + acc, result, 0.0)

    def globalGdpContribution(self, data, continents, startYear, endYear, **kwargs):
        def calculateRecordTotal(record):
            yearsRange = list(map(str, range(int(startYear), int(endYear) + 1)))
            return reduce(lambda acc, yr: acc + float(record.get(yr, 0) or 0), yearsRange, 0.0)

        worldTotalRange = reduce(lambda acc, record: acc + calculateRecordTotal(record), data, 0.0)

        def calculateShare(contName):
            contSum = self.continentGdp(data, contName, startYear, endYear)
            sharePct = (contSum / worldTotalRange * 100) if worldTotalRange > 0 else 0
            return {
                "type": "Global Contribution",
                "continent": contName,
                "rangeSum": round(contSum, 2),
                "globalContribution": round(sharePct, 2)
            }

        return list(map(calculateShare, continents))

    def GlobalGDPTrends(self, data, startYear, endYear, **kwargs):
        def calcforaYear(year):
            return reduce(lambda acc, x: acc + float(x.get(year, 0) or 0), data, 0)

        yearsRange = list(map(str, range(int(startYear), int(endYear) + 1)))
        return list(map(lambda year: {"year": year, "totalGDP": calcforaYear(year)}, yearsRange))

    def fastestGrowingContinent(self, data, continents, startYear, endYear, **kwargs):
        def calculateGrowthShare(contName):
            filtered = list(filter(lambda r: r.get("Continent") == contName, data))
            startYearSum = reduce(
                lambda acc, record: acc + float(record.get(str(startYear), 0) or 0), filtered, 0.0
            )
            endYearSum = reduce(
                lambda acc, record: acc + float(record.get(str(endYear), 0) or 0), filtered, 0.0
            )
            growthPct = ((endYearSum - startYearSum) / startYearSum * 100) if startYearSum > 0 else 0
            return {
                "type": "Fastest Growing Continent",
                "continent": contName,
                "startYearGDP": round(startYearSum, 2),
                "endYearGDP": round(endYearSum, 2),
                "growthPercent": round(growthPct, 2),
                "absoluteGrowth": round(endYearSum - startYearSum, 2)
            }

        growth_data = list(map(calculateGrowthShare, continents))
        return sorted(growth_data, key=lambda x: x["growthPercent"], reverse=True)

    def countriesWithConsistentDecline(self, data, lastXYears, **kwargs):
        def checkCountryDecline(record):
            country = record.get("Country Name")
            continent = record.get("Continent")

            years = sorted(
                list(map(int, filter(lambda x: isinstance(x, str) and x.isdigit(), record.keys()))),
                reverse=True
            )[:lastXYears]

            if len(years) < lastXYears:
                return None

            def createDeclineDetail(i):
                current_year = years[i]
                next_year = years[i + 1]
                current_gdp = float(record.get(str(current_year), 0) or 0)
                next_gdp = float(record.get(str(next_year), 0) or 0)
                decline_pct = ((next_gdp - current_gdp) / next_gdp * 100) if next_gdp > 0 else 0
                return {
                    "from_year": next_year,
                    "to_year": current_year,
                    "gdp": (next_gdp, current_gdp),
                    "decline_percent": round(decline_pct, 2),
                    "is_declining": current_gdp < next_gdp
                }

            decline_details = list(map(createDeclineDetail, range(len(years) - 1)))
            all_declining = len(list(filter(lambda x: x["is_declining"], decline_details))) == len(decline_details)

            if not all_declining or not decline_details:
                return None

            total_decline = ((float(record.get(str(years[0]), 0) or 0) - float(record.get(str(years[-1]), 0) or 0)) /
                             float(record.get(str(years[-1]), 0) or 1)) * 100

            return {
                "country": country,
                "continent": continent,
                "consistentDeclineYears": lastXYears,
                "yearRange": f"{years[-1]} to {years[0]}",
                "declineDetails": decline_details,
                "totalDeclinePercent": round(total_decline, 2)
            }

        decline_countries = list(filter(
            lambda x: x is not None,
            map(checkCountryDecline, data)
        ))
        return sorted(decline_countries, key=lambda x: abs(x["totalDeclinePercent"]), reverse=True)

    def average_gdp_by_continent(self, data, continents, startYear, endYear, **kwargs):
        years = list(map(str, range(int(startYear), int(endYear) + 1)))

        def calc_continent_avg(continent):
            filtered = list(filter(lambda r: r.get("Continent") == continent, data))
            country_averages = list(map(
                lambda r: reduce(lambda acc, y: acc + float(r.get(y, 0) or 0), years, 0.0) / len(years),
                filtered
            ))
            continent_avg = reduce(lambda acc, x: acc + x, country_averages, 0.0) / len(country_averages) if country_averages else 0.0
            return {
                "continent": continent,
                "startYear": str(startYear),
                "endYear": str(endYear),
                "average_gdp": round(continent_avg, 2)
            }

        return list(map(calc_continent_avg, continents))

    def gdp_growth_rate(self, data, continent, startYear, endYear, **kwargs):
        start_year = str(startYear)
        end_year = str(endYear)

        filtered = list(filter(
            lambda r: r.get("Continent") == continent and start_year in r and end_year in r, data
        ))

        def calc_growth(r):
            gdp_start = float(r.get(start_year, 0) or 0)
            gdp_end = float(r.get(end_year, 0) or 0)
            growth = ((gdp_end - gdp_start) / gdp_start) * 100 if gdp_start else None
            return {
                "country": r.get("Country Name"),
                "continent": continent,
                "start_year": start_year,
                "end_year": end_year,
                "gdp_start": gdp_start,
                "gdp_end": gdp_end,
                "growth_rate": round(growth, 2) if growth is not None else None
            }

        growth_list = list(map(calc_growth, filtered))
        return sorted(
            growth_list,
            key=lambda r: r["growth_rate"] if r["growth_rate"] is not None else float('inf')
        )


# ── In-memory sink: captures results instead of printing/rendering ──────────

class MemorySink:
    """
    Replaces ConsoleWriter / RenderChart for web use.
    Stores results in self.results for the API to return as JSON.
    """
    def __init__(self):
        self.results: List[dict] = []

    def write(self, records: List[dict]) -> None:
        self.results = records
