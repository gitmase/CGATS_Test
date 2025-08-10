# Obsolete file - use loadJson.py instead
import json
import uuid
import logging
import os
import argparse
from pathlib import Path
from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Text, DateTime,
    ForeignKeyConstraint, PrimaryKeyConstraint, text
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from datetime import datetime
from collections import Counter

# Configuration
TARGET_SCHEMA = 'color_measurement'
Base = declarative_base()

Log_File = Path(__file__).parent / "import_json.log" if "__file__" in globals() else Path.cwd() / "import_json.log"
DB_URI = "postgresql+psycopg2://postgres:postgres@10.211.55.9:5432/postgres"

def setup_logging(logfile: Path, debug: bool = False, verbose: bool = False):
    log_level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(logfile, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def log(msg: str, level: int = logging.INFO):
    logging.log(level, msg)

class DescriptiveData(Base):
    __tablename__ = 'descriptive_data'
    __table_args__ = {'schema': TARGET_SCHEMA}

    measurement_id = Column(UUID(as_uuid=True), default=uuid.uuid4, primary_key=True)
    originator = Column(Text)
    description = Column(Text)
    creation_date = Column(Text)
    device_model = Column(Text)
    measurement_illumination = Column(Text)
    measurement_angle = Column(Text)
    number_of_fields = Column(Integer)
    data_format = Column(Text)
    number_of_sets = Column(Integer)
    row_length = Column(Integer)
    measurement_date = Column(DateTime(timezone=True), default=func.now())

    def __repr__(self):
        return (f"<DescriptiveData(measurement_id={self.measurement_id}, originator={self.originator}, ...)>")

class MeasurementData(Base):
    __tablename__ = 'measurement_data'
    __table_args__ = (
        PrimaryKeyConstraint('measurement_id', 'sample_id'),
        ForeignKeyConstraint(['measurement_id'], [f'{TARGET_SCHEMA}.descriptive_data.measurement_id']),
        {'schema': TARGET_SCHEMA}
    )

    measurement_id = Column(UUID(as_uuid=True), default=uuid.uuid4)
    sample_id = Column(Integer)
    cmyk_c = Column(Float)
    cmyk_m = Column(Float)
    cmyk_y = Column(Float)
    cmyk_k = Column(Float)
    xyz_x = Column(Float)
    xyz_y = Column(Float)
    xyz_z = Column(Float)
    lab_l = Column(Float)
    lab_a = Column(Float)
    lab_b = Column(Float)
    # Spectral columns
    spectral_380 = Column(Float)
    spectral_390 = Column(Float)
    spectral_400 = Column(Float)
    spectral_410 = Column(Float)
    spectral_420 = Column(Float)
    spectral_430 = Column(Float)
    spectral_440 = Column(Float)
    spectral_450 = Column(Float)
    spectral_460 = Column(Float)
    spectral_470 = Column(Float)
    spectral_480 = Column(Float)
    spectral_490 = Column(Float)
    spectral_500 = Column(Float)
    spectral_510 = Column(Float)
    spectral_520 = Column(Float)
    spectral_530 = Column(Float)
    spectral_540 = Column(Float)
    spectral_550 = Column(Float)
    spectral_560 = Column(Float)
    spectral_570 = Column(Float)
    spectral_580 = Column(Float)
    spectral_590 = Column(Float)
    spectral_600 = Column(Float)
    spectral_610 = Column(Float)
    spectral_620 = Column(Float)
    spectral_630 = Column(Float)
    spectral_640 = Column(Float)
    spectral_650 = Column(Float)
    spectral_660 = Column(Float)
    spectral_670 = Column(Float)
    spectral_680 = Column(Float)
    spectral_690 = Column(Float)
    spectral_700 = Column(Float)
    spectral_710 = Column(Float)
    spectral_720 = Column(Float)
    spectral_730 = Column(Float)
    spectral_740 = Column(Float)
    spectral_750 = Column(Float)
    spectral_760 = Column(Float)
    spectral_770 = Column(Float)
    spectral_780 = Column(Float)

    def __repr__(self):
        return (f"<MeasurementData(measurement_id={self.measurement_id}, sample_id={self.sample_id}, ...)>")

def ensure_schema(engine, schema_name: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"),
            {"schema_name": schema_name}
        )
        if result.fetchone():
            log(f"Schema '{schema_name}' exists.", logging.INFO)
        else:
            log(f"Schema '{schema_name}' does not exist. Creating...", logging.WARNING)
            # Uncomment to create schema if needed:
            # conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            # conn.commit()

def process_file(json_file: Path, guid: uuid.UUID):
    engine = create_engine(DB_URI, echo=False)
    ensure_schema(engine, TARGET_SCHEMA)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    with open(json_file, "r") as f:
        data = json.load(f)

    desc = data["descriptive_data"]
    measurements = data["measurement_data"]

    cleaned_measurements = []
    for row in measurements:
        row["MEASUREMENT_ID"] = guid
        if not row.get("MEASUREMENT_ID"):
            raise ValueError(f"Missing MEASUREMENT_ID in row: {row}")
        if "SAMPLE_ID" not in row:
            raise ValueError(f"Missing SAMPLE_ID in row: {row}")
        cleaned_measurements.append(row)

    pairs = [(r["MEASUREMENT_ID"], r["SAMPLE_ID"]) for r in cleaned_measurements]
    dupes = [k for k, v in Counter(pairs).items() if v > 1]
    if dupes:
        raise ValueError(f"Duplicate (measurement_id, sample_id) pairs found: {dupes}")

    descriptive_entry = DescriptiveData(
        measurement_id=guid,
        originator=desc["ORIGINATOR"],
        description=desc["DESCRIPTION"],
        creation_date=desc["CREATION_DATE"],
        device_model=desc["DEVICE_MODEL"],
        measurement_illumination=desc["MEASUREMENT_ILLUMINATION"],
        measurement_angle=desc["MEASUREMENT_ANGLE"],
        number_of_fields=int(desc["NUMBER_OF_FIELDS"]),
        data_format=desc["DATA_FORMAT"],
        number_of_sets=int(desc["NUMBER_OF_SETS"]),
        row_length=int(desc["ROW_LENGTH"]),
        measurement_date=datetime.strptime(desc["MEASUREMENT_DATE"], "%Y-%m-%d").date()
    )

    try:
        session.add(descriptive_entry)
        session.commit()
        session.refresh(descriptive_entry)
        log("DescriptiveData entry committed successfully.", logging.INFO)
    except SQLAlchemyError as e:
        session.rollback()
        log(f"Error inserting DescriptiveData: {e}", logging.ERROR)
        raise

    for row in cleaned_measurements:
        base_fields = {
            "measurement_id": row["MEASUREMENT_ID"],
            "sample_id": row["SAMPLE_ID"],
            "cmyk_c": row.get("CMYK_C"),
            "cmyk_m": row.get("CMYK_M"),
            "cmyk_y": row.get("CMYK_Y"),
            "cmyk_k": row.get("CMYK_K"),
            "xyz_x": row.get("XYZ_X"),
            "xyz_y": row.get("XYZ_Y"),
            "xyz_z": row.get("XYZ_Z"),
            "lab_l": row.get("LAB_L"),
            "lab_a": row.get("LAB_A"),
            "lab_b": row.get("LAB_B"),
        }
        spectral_fields = {
            f'spectral_{wavelength}': row.get(f"SPECTRAL_{wavelength}", None)
            for wavelength in range(380, 781, 10)
        }
        measurement_entry = MeasurementData(**base_fields, **spectral_fields)
        try:
            session.add(measurement_entry)
            session.commit()
            session.refresh(measurement_entry)
            log(f"Measurement entry for sample_id {measurement_entry.sample_id} committed.", logging.INFO)
        except SQLAlchemyError as e:
            session.rollback()
            log(f"Error inserting MeasurementData: {e}", logging.ERROR)
            raise
    session.close()

def main():
    parser = argparse.ArgumentParser(description="Process input and output files with a path.")
    parser.add_argument("--path", default="/Users/aps/Docs/TestData/", help="The folder path")
    parser.add_argument("--json_file", default="printerTest.output.json", help="The input file name")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    setup_logging(Log_File, debug=args.debug, verbose=args.verbose)
    json_file = Path(args.path) / args.json_file
    guid = uuid.uuid4()
    log(f"Generated GUID: {guid}", logging.INFO)
    log(f"Processing file: {json_file}", logging.INFO)
    process_file(json_file, guid)
    log("✅ All measurement records inserted successfully.", logging.INFO)

if __name__ == "__main__":
    main()
