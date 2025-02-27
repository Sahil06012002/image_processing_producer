import io
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import csv
import uuid
from io import StringIO
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy.exc import SQLAlchemyError
import json
from models import SessionLocal, ProcessingRequest, Product
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Image Processing System")

ssl_cafile = os.getenv("SSL_CAFILE")
ssl_certfile = os.getenv("SSL_CERTFILE")
ssl_keyfile = os.getenv("SSL_KEYFILE")

producer = KafkaProducer(
    bootstrap_servers="kafka-202eeccb-kr1798977-d28e.h.aivencloud.com:28772",
    security_protocol="SSL",
    ssl_cafile=ssl_cafile,
    ssl_certfile=ssl_certfile,
    ssl_keyfile=ssl_keyfile
)

def validate_csv_format(csv_content):
    reader = csv.reader(StringIO(csv_content))
    header = next(reader)
    if len(header) != 3:
        raise ValueError("CSV must have exactly 3 columns")
    
    for row in reader:
        if len(row) != 3:
            raise ValueError("Each row must have exactly 3 columns")
        if not all(row):
            raise ValueError("All fields must be non-empty")

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    print("incoming req")
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    content = await file.read()
    csv_content = content.decode()
    
    try:
        validate_csv_format(csv_content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    request_id = str(uuid.uuid4())
    
    db = SessionLocal()
    try:
        reader = csv.reader(StringIO(csv_content))
        next(reader)  
        message_data = []
        for row in reader:
            # append the s no : this could also be same, number of the product
            product_name = row[1]
            request = ProcessingRequest(request_id=request_id,product_name = product_name, status="pending")
            db.add(request)
            urls = (row[2].split(","))

            message_data.append((product_name,urls))

            product = Product(
                serial_number=row[0],
                product_name=row[1],
                input_image_urls=row[2],
                request_id=request_id
            )
            db.add(product)
        
        db.commit()

        #send message to kafka
        TOPIC_NAME = os.getenv('TOPIC_NAME')   
        for product_name,urls in message_data:
            message = {
                "request_id" : request_id,
                "product_name" : product_name,
                "urls" : urls
            }
            try:
                producer.send(TOPIC_NAME, key=request_id.encode("utf-8"), value=json.dumps(message).encode("utf-8"))
            except KafkaError as e:
                raise HTTPException(status_code=500, detail=f"Kafka message sending failed: {str(e)}")
        
    except SQLAlchemyError as e:
        db.rollback() 
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

    finally:
        db.close()
    
    return JSONResponse({
        "request_id": request_id,
        "message": "CSV file accepted for processing"
    })

@app.get("/status/{request_id}")
async def get_status(request_id: str):
    db = SessionLocal()
    try:
        requests = db.query(ProcessingRequest).filter(
            ProcessingRequest.request_id == request_id
        ).all()
        
        if not requests:
            raise HTTPException(status_code=404, detail="Request ID not found")
        
        total_entries = len(requests)
        completed_entries = len([r for r in requests if r.status == "completed"])
        
        if completed_entries > 0:
            completion_percentage = (completed_entries / total_entries) * 100
            status = "Completed %s %%" % completion_percentage if completion_percentage < 100 else "Completed"
        else:
            status = "Pending"
        
        return {
            "status": status,
            "created_at": requests[0].created_at,
            "updated_at": requests[0].updated_at,
            "completed_requests": completed_entries,
            "total_requests": total_entries,
            "completion_percentage": completion_percentage if completed_entries > 0 else 0
        }
    
    finally:
        db.close()

@app.get("/export/{request_id}")
async def export_csv(request_id: str):
    db = SessionLocal()
    try:
        products = db.query(Product).filter(Product.request_id == request_id).all()

        if not products:
            raise HTTPException(status_code=404, detail="Request ID not found or no products available.")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Serial Number", "Product Name", "Input Image URLs", "Output Image URLs"])

        for product in products:
            writer.writerow([product.serial_number, product.product_name, product.input_image_urls, product.output_image_urls])

        output.seek(0)
        return StreamingResponse(output, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=request_{request_id}.csv"})
    
    finally:
        db.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)