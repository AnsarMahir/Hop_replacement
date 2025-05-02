from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import shutil
import tempfile
import os
import yaml
from validator import ExcelValidator, load_config  # assuming your validator class is in validator.py
from script import process_excel_for_insertion  # assuming you have a callable main for insert

app = FastAPI()

@app.post("/process-excel/")
async def process_excel(
    excel_file: UploadFile = File(...),
    config_file: UploadFile = File(...)
):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save uploaded Excel file
            excel_path = os.path.join(tmpdir, excel_file.filename)
            with open(excel_path, "wb") as f:
                shutil.copyfileobj(excel_file.file, f)

            # Save uploaded config file
            config_path = os.path.join(tmpdir, config_file.filename)
            with open(config_path, "wb") as f:
                shutil.copyfileobj(config_file.file, f)

            # Load config
            config = load_config(config_path)
            if not config:
                raise HTTPException(status_code=400, detail="Failed to load configuration")

            # Validate Excel
            validator = ExcelValidator(config)
            validation_result = validator.validate_file(excel_path)

            if not validation_result["is_valid"]:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "message": "Validation failed", "details": validation_result}
                )

            # Proceed to insertion
            insert_success = process_excel_for_insertion(excel_path, config_path, 599, 599)

            if not insert_success:
                raise HTTPException(status_code=500, detail="Data insertion failed")

            return {"success": True, "message": "Validation passed and data inserted successfully"}

    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
