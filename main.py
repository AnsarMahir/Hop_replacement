from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import shutil
import tempfile
import os
import yaml
from validator import ExcelValidator, load_config           # your validation
from script import process_excel_for_insertion              # your insert wrapper

app = FastAPI()

@app.post("/process-excel/")
async def process_excel(
    excel_file:               UploadFile = File(...),
    validation_config_file:   UploadFile = File(...),
    insertion_config_file:    UploadFile = File(...),
    customer_id:              int        = Form(...),
    application_id:           int        = Form(...)
):
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # 1) Save uploaded Excel
            excel_path = os.path.join(tmpdir, excel_file.filename)
            with open(excel_path, "wb") as f:
                shutil.copyfileobj(excel_file.file, f)

            # 2) Save validation config
            val_cfg_path = os.path.join(tmpdir, validation_config_file.filename)
            with open(val_cfg_path, "wb") as f:
                shutil.copyfileobj(validation_config_file.file, f)

            # 3) Save insertion config
            ins_cfg_path = os.path.join(tmpdir, insertion_config_file.filename)
            with open(ins_cfg_path, "wb") as f:
                shutil.copyfileobj(insertion_config_file.file, f)

            # 4) Load & run validation
            val_config = load_config(val_cfg_path)
            if not val_config:
                raise HTTPException(status_code=400, detail="Failed to load validation config")

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

            # 5) Validation passed → run insertion
            insert_success = process_excel_for_insertion(
                excel_path=excel_path,
                config_path=ins_cfg_path,
                customer_id=customer_id,
                application_id=application_id
            )

            if not insert_success:
                raise HTTPException(status_code=500, detail="Data insertion failed")

            return {"success": True, "message": "Validation passed and data inserted successfully"}

    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
