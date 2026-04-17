"""
API Routes
==========
POST /upload      – accept Excel/CSV/JSON file, return file_id
POST /analyze     – run analysis, return job result
GET  /results/{id} – fetch cached result
GET  /health       – liveness check
"""

import os
import uuid
import json
import logging
import tempfile
import shutil
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
import io
import csv as csv_module

from app.services.analyzer import GDPAnalyzerService

logger = logging.getLogger(__name__)
router = APIRouter()

# ── In-memory stores (replace with Redis/DB for production) ─────────────────
_uploaded_files: dict = {}   # file_id → temp file path
_results_cache: dict = {}    # result_id → output dict

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".json"}


def _save_upload(upload: UploadFile) -> tuple[str, str]:
    """Save uploaded file to disk, return (file_id, path)."""
    ext = os.path.splitext(upload.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {ALLOWED_EXTENSIONS}"
        )
    file_id = str(uuid.uuid4())
    dest_path = os.path.join(UPLOAD_DIR, f"{file_id}{ext}")
    with open(dest_path, "wb") as f:
        shutil.copyfileobj(upload.file, f)
    return file_id, dest_path


def _cleanup_old_files(max_age_seconds: int = 3600):
    """Remove uploads older than max_age_seconds (background task)."""
    import time
    now = time.time()
    for fname in os.listdir(UPLOAD_DIR):
        fpath = os.path.join(UPLOAD_DIR, fname)
        try:
            if os.path.isfile(fpath) and (now - os.path.getmtime(fpath)) > max_age_seconds:
                os.remove(fpath)
                logger.info(f"Cleaned up old upload: {fpath}")
        except Exception:
            pass


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok", "service": "GDP Analyzer API"}


@router.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """
    Accept an Excel (.xlsx/.xls), CSV, or JSON file.
    Returns a file_id to be used in /analyze.
    """
    try:
        file_id, path = _save_upload(file)
        _uploaded_files[file_id] = path
        background_tasks.add_task(_cleanup_old_files)
        logger.info(f"Uploaded file saved: {path} (id={file_id})")
        return {
            "file_id": file_id,
            "filename": file.filename,
            "message": "File uploaded successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.post("/analyze")
async def analyze(body: dict):
    """
    Body:
      {
        "file_id": "<uuid>",
        "config": { ...core.json structure... }
      }
    Runs the Python analysis pipeline and returns results + chart hints.
    """
    file_id: Optional[str] = body.get("file_id")
    config: Optional[dict] = body.get("config")

    if not file_id:
        raise HTTPException(status_code=400, detail="'file_id' is required")
    if not config:
        raise HTTPException(status_code=400, detail="'config' is required")

    file_path = _uploaded_files.get(file_id)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File not found for file_id '{file_id}'. Re-upload the file."
        )

    try:
        service = GDPAnalyzerService()
        service.load_file(file_path)
        results = service.compute_statistics(file_path, config)
        output = service.generate_output(results, config)

        # Cache result
        result_id = str(uuid.uuid4())
        _results_cache[result_id] = output
        output["result_id"] = result_id

        logger.info(f"Analysis complete: {output['analysis_name']} | {output['record_count']} records")
        return output

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Analysis error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/results/{result_id}")
async def get_results(result_id: str):
    """Fetch a previously computed result by ID."""
    result = _results_cache.get(result_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Result '{result_id}' not found or expired")
    return result


@router.get("/results/{result_id}/download")
async def download_results(result_id: str):
    """Stream results as a CSV file download."""
    result = _results_cache.get(result_id)
    if not result:
        raise HTTPException(status_code=404, detail="Result not found")

    records = result.get("results", [])
    if not records:
        raise HTTPException(status_code=404, detail="No results to download")

    output = io.StringIO()
    writer = csv_module.DictWriter(output, fieldnames=records[0].keys())
    writer.writeheader()
    for row in records:
        # Flatten nested structures (e.g. gdp tuples in decline analysis)
        flat = {k: (str(v) if isinstance(v, (list, tuple, dict)) else v) for k, v in row.items()}
        writer.writerow(flat)
    output.seek(0)

    filename = f"{result.get('analysis_name', 'results').replace(' ', '_')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/config/default")
async def get_default_config():
    """Returns the default core.json so the UI can pre-populate the editor."""
    default_config_path = os.path.join(os.path.dirname(__file__), "..", "core.json")
    try:
        with open(default_config_path) as f:
            return json.load(f)
    except Exception:
        # Fallback minimal config
        return {
            "input_provider": "csv",
            "FunctionOption": "1",
            "parameters": {
                "continent": "Asia",
                "year": 2023,
                "YearRange": {"startYear": 2010, "EndYear": 2021},
                "continents": ["Asia", "Europe", "Africa", "North America", "South America", "Oceania"],
                "lastXyears": 3
            },
            "chart": {"params": {}}
        }
