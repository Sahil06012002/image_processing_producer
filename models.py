import os
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()
load_dotenv()


class ProcessingRequest(Base):
    __tablename__ = 'processing_requests'
    
    id = Column(Integer, primary_key=True)
    request_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    status = Column(String, nullable=False)  # pending, processing, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    serial_number = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    input_image_urls = Column(String, nullable=False)  # Comma-separated URLs
    output_image_urls = Column(String)  # Comma-separated URLs
    request_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

engine = create_engine(os.getenv('DATABASE_URL'))
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)