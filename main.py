from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import shutil
import tempfile
import os
from validator import ExcelValidator, load_config           # your validation
from script import process_excel_for_insertion              # your insert wrapper

app = FastAPI()

# Paths to your local config files
VALIDATION_CONFIG_PATH = "validation_config.yaml"
INSERTION_CONFIG_PATH = "config.yaml"

@app.post("/process-excel/")
async def process_excel(
    excel_file:     UploadFile = File(...),
    customer_id:    int        = Form(...),
    application_id: int        = Form(...)
):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1) Save uploaded Excel
            excel_path = os.path.join(tmpdir, excel_file.filename)
            with open(excel_path, "wb") as f:
                shutil.copyfileobj(excel_file.file, f)

            # 2) Load validation config
            if not os.path.exists(VALIDATION_CONFIG_PATH):
                raise HTTPException(status_code=500, detail="Validation config file not found")

            val_config = load_config(VALIDATION_CONFIG_PATH)
            if not val_config:
                raise HTTPException(status_code=400, detail="Failed to load validation config")

            # 3) Run validation
            validator = ExcelValidator(val_config)
            validation_result = validator.validate_file(excel_path)
            if not validation_result["is_valid"]:
                error_messages = validation_result["errors"]
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "message": "; ".join(error_messages) if error_messages else "Validation failed"
                    }
                )

            # 4) Validation passed → run insertion
            if not os.path.exists(INSERTION_CONFIG_PATH):
                raise HTTPException(status_code=500, detail="Insertion config file not found")

            insert_success = process_excel_for_insertion(
                excel_path=excel_path,
                config_path=INSERTION_CONFIG_PATH,
                customer_id=customer_id,
                application_id=application_id
            )

            if not insert_success:
                raise HTTPException(status_code=500, detail="Data insertion failed")

            return {"success": True, "message": "Validation passed and data inserted successfully"}

    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
