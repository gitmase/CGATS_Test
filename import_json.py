import json
import uuid
import logging
import pickle
import psycopg2
import os
import argparse
from sqlalchemy import (
    ForeignKeyConstraint, create_engine, Column, Integer, Float, String, Text, Date, DateTime,
    ForeignKey, PrimaryKeyConstraint, Uuid, text)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from datetime import datetime

from sqlalchemy.orm import declarative_base, sessionmaker,Mapped, relationship

from sqlalchemy.types import Uuid
from collections import Counter

global DB_URI
global target_schema
global Base
global guid
global log_file 
global debug
global verbose

target_schema = 'color_measurement'

Base = declarative_base()

# Configuration
def log(msg, logfile=None):
    """
    Logs a message to stdout and optionally to a file.
    :param msg: The message to log
    :param verbose: If True, print [INFO] level messages
    :param debug: If True, print [DEBUG] level messages
    :param logfile: Path to file to append log messages to
    """
    log_line = ""
    if debug:
        log_line = f"[DEBUG] {msg}"
    elif verbose:
        log_line = f"[INFO] {msg}"

    if log_line:
        print(log_line)
        if logfile:
            try:
                with open(logfile, "a", encoding="utf-8") as f:
                    f.write(log_line + "\n")
            except Exception as e:
                print(f"[ERROR] Failed to write to log file: {e}")



def quote(string):
    return f"'{string}'"


# Define table model
class JunkData(Base):
    __tablename__ = 'junk'
    __table_args__ = {'schema': target_schema}

    c1 = Column(Text, primary_key=True)
    c2 = Column(Integer)
    c3 = Column(UUID(as_uuid=True), default=uuid.uuid4)

    def __repr__(self):
        return f"<JunkData(c1={self.c1}, c2={self.c2}, c3={self.c3})>"



class DescriptiveData(Base):
    __tablename__ = 'descriptive_data'
    __table_args__ = {'schema': target_schema}

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
        return (f"<DescriptiveData(measurement_id={self.measurement_id}, "
                f"originator={self.originator}, description={self.description}, "
                f"creation_date={self.creation_date}, device_model={self.device_model}, "
                f"measurement_illumination={self.measurement_illumination}, "
                f"measurement_angle={self.measurement_angle}, number_of_fields={self.number_of_fields}, "
                f"data_format={self.data_format}, number_of_sets={self.number_of_sets}, "
                f"row_length={self.row_length}, measurement_date={self.measurement_date})>")            


class MeasurementData(Base):
    __tablename__ = 'measurement_data'
    __table_args__ = (
        PrimaryKeyConstraint('measurement_id', 'sample_id'),
        ForeignKeyConstraint(['measurement_id'], [f'{target_schema}.descriptive_data.measurement_id']),
        {'schema': target_schema}
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

    # Spectral columns (optional in data, but always defined in schema)
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
        return (f"<MeasurementData(measurement_id={self.measurement_id}, sample_id={self.sample_id}, "
                f"cmyk_c={self.cmyk_c}, cmyk_m={self.cmyk_m}, cmyk_y={self.cmyk_y}, cmyk_k={self.cmyk_k}, "
                f"xyz_x={self.xyz_x}, xyz_y={self.xyz_y}, xyz_z={self.xyz_z}, "
                f"lab_l={self.lab_l}, lab_a={self.lab_a}, lab_b={self.lab_b}, "
                f"spectral_380={self.spectral_380}, spectral_390={self.spectral_390}, spectral_400={self.spectral_400}, "
                f"... etc ... )>")  



# Ensure schema exists
def ensure_schema(engine, schema_name):
    with engine.connect() as conn:
        result = conn.execute(
    text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"),
    {"schema_name": schema_name})
        if result.fetchone():
            log(f"Schema '{schema_name}' exists.", logfile=log_file)
        else:
            log(f"Schema '{schema_name}' does not exist. Creating...", logfile=log_file)

        # conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        # conn.commit()


def process_file(json_file, session=None):

    # --- Database Setup ---
    engine = create_engine(DB_URI, echo=False)
    ensure_schema(engine, target_schema)

    # Create tables
    Base.metadata.create_all(engine)


    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()# Connect and perform operations within the specified schema

    if not session.is_active:
        log("Session is not active. Starting a new session.", logfile=log_file)
        print("Session is not active. Starting a new session.")
        exit()

    # --- Load JSON ---
    with open(json_file, "r") as f:
        data = json.load(f)

    log(f"Loaded JSON keys: {list(data.keys())}", logfile=log_file)

    desc = data["descriptive_data"]
    measurements = data["measurement_data"]

    # --- Normalize and Validate Measurement Data ---
    cleaned_measurements = []

    for row in measurements:
        row["MEASUREMENT_ID"] = guid # row.get("MEASUREMENT_ID") or row.pop("MEASUREMENT_ID:", None)
        if not row["MEASUREMENT_ID"]:
            raise ValueError(f"Missing MEASUREMENT_ID in row: {row}")
        if "SAMPLE_ID" not in row:
            raise ValueError(f"Missing SAMPLE_ID in row: {row}")
        cleaned_measurements.append(row)

        # Uniqueness check
        pairs = [(r["MEASUREMENT_ID"], r["SAMPLE_ID"]) for r in cleaned_measurements]
        
        dupes = [k for k, v in Counter(pairs).items() if v > 1]
        if dupes:
            raise ValueError(f"Duplicate (measurement_id, sample_id) pairs found: {dupes}")

        log(f"✅ Loaded {len(cleaned_measurements)} records with {len(set(pairs))} unique sample pairs.", logfile=log_file)


    # --- Insert JunkData Record for Testing ---
    junk_entry = JunkData(c1=quote(guid), c2=100, c3=guid)
    log(f"Inserting junk entry: {junk_entry}", logfile=log_file)

    if session.is_active:
        session.rollback()  # Ensure no active transaction
        log("JunkData session is currently within an active transaction.", logfile=log_file)
        try:
            # session.rollback()
            session.add(junk_entry)
            session.commit()
            session.refresh(junk_entry)
            log("JunkData entry committed successfully.", logfile=log_file)
        except:
            session.merge(junk_entry)
            session.commit()
            log("JunkData entry merged successfully.",  logfile=log_file)
    else:
        log("The session is not within an active transaction.", logfile=log_file)

    # Verify
    result = session.query(JunkData).filter_by(c1=quote(guid)).first()
    if result:
        log(f"✅ Record confirmed: {result.c1}, {result.c2}, {result.c3}", logfile=log_file)
    else:
        log("❌ Record not found in table after commit.", logfile=log_file)


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

    print(f"Inserting descriptive entry: {descriptive_entry}")
    # log(f"Inserting descriptive entry: {descriptive_entry}", verbose=True, logfile="/Users/aps/Docs/TestData/process.log")    

    if session.is_active:
        log("DescriptiveData session is currently within an active transaction.", logfile=log_file)
        print(f"DescriptiveData session is currently within an active transaction.")
        try:
            session.add(descriptive_entry)
            session.commit()
            session.refresh(descriptive_entry)
            log("DescriptiveData entry committed successfully.", logfile=log_file)
        except:
            session.merge(descriptive_entry)
            session.commit()
            session.refresh(descriptive_entry)
            log("DescriptiveData entry merged successfully.", logfile=log_file)
    else:
        print("The session is not within an active transaction.")
        log("The session is not within an active transaction.", logfile=log_file)


    # --- Insert Measurement Data (missing spectral fields = None) ---

    #spectral_keys = [f"SPECTRAL_{i}" for i in range(380, 781, 10)]

    for row in cleaned_measurements:
        log(f"Processing measurement row: MEASUREMENT_ID={row['MEASUREMENT_ID']}, SAMPLE_ID={row['SAMPLE_ID']}", logfile=log_file)
        print
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
        log(f"Inserting measurement: {measurement_entry.measurement_id}, sample: {measurement_entry.sample_id}, spectral_380: {measurement_entry.spectral_380}", logfile=log_file)
        log(f"Measurement entry details: {measurement_entry}", logfile=log_file)

        if session.is_active:
            log("Measurement session is currently within an active transaction.", logfile=log_file)
            try:
                session.add(measurement_entry)
                session.commit()
                session.refresh(measurement_entry)    
                log("Measurement entry committed successfully.", logfile=log_file)
            except:
                session.merge(measurement_entry)
                session.commit()
                session.refresh(measurement_entry)
                log(f"Measurement entry for sample_id {measurement_entry.sample_id} merged successfully.", logfile=log_file)
        else:
            log("The session is not within an active transaction.", logfile=log_file)
    # Final commit and close session
        session.commit()
        session.close()


def main():
    # Global Variables
    global debug
    debug = False
    global verbose
    verbose = False

    start_folder = os.path.dirname(os.path.abspath(__file__))
    print("Script location:", start_folder)

    parser = argparse.ArgumentParser(description="Process input and output files with a path.")
    parser.add_argument("--path", default="/Users/aps/Docs/TestData/", help="The folder path (default: current directory)")
    parser.add_argument("--json_file", default="printerTest.output.json", help="The input file name (default: input.txt)")
    parser.add_argument("--log_file", default="process.log", help="The log file name (default: process.log)")
    # Debugging and Verbosity Flags
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()
    
    if args.debug:
        debug = True
        print("Debug mode is ON")
    
    if args.verbose:
        verbose = True
        print("Verbose output enabled")

    path = args.path
    json_file = path + args.json_file
    global log_file
    log_file = start_folder + '/' + args.log_file
    log(f"json file: {json_file}", logfile=log_file)

    global guid
    guid = uuid.uuid4()
    print(f"Generated GUID: {guid}")

    # DB Configuration
    global DB_URI
    DB_URI = "postgresql+psycopg2://postgres:postgres@10.211.55.9:5432/postgres"
    

    # Process the input file and load table data
    process_file(json_file)
    
    print("✅ All measurement records inserted successfully.")


if __name__ == "__main__":
    main()

