import json
import uuid
import logging
import argparse
from pathlib import Path
from datetime import datetime
from collections import Counter

# Loads JSON data into a dynamic SQLAlchemy schema in PostgreSQL.
# Supports both descriptive and measurement data with flexible column mapping
# Version 1.0.1

def _extract_measurement_id_from_json(desc: dict, measurements: list) -> uuid.UUID | None:
    """
    Try to find a measurement_id in the JSON payload.
    Accepts 'MEASUREMENT_ID' or 'measurement_id' in desc or rows.
    Ensures all discovered IDs (if multiple) are identical; otherwise raises.
    Returns uuid.UUID or None if not present.
    """
    candidates = set()

    # From descriptive section
    for key in ("MEASUREMENT_ID", "measurement_id"):
        val = (desc or {}).get(key)
        if val:
            candidates.add(str(val).strip())

    # From measurement rows
    for row in (measurements or []):
        for key in ("MEASUREMENT_ID", "measurement_id"):
            val = row.get(key)
            if val:
                candidates.add(str(val).strip())

    if not candidates:
        return None

    if len(candidates) > 1:
        raise ValueError(f"Conflicting MEASUREMENT_ID values in JSON: {sorted(candidates)}")

    only = next(iter(candidates))
    try:
        return uuid.UUID(only)
    except Exception as e:
        raise ValueError(f"Invalid MEASUREMENT_ID format in JSON: {only}") from e


from sqlalchemy import (
    create_engine, Column, Integer, Float, Text, DateTime,
    ForeignKey, text
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func

# ----------------------------
# Config / Logging
# ----------------------------
DEFAULT_SCHEMA = "color_data" # "color_measurement"
LOG_FILE = Path(__file__).parent / "import_json.log" if "__file__" in globals() else Path.cwd() / "import_json.log"
DB_URI = "postgresql+psycopg2://pgadmin:postgres@10.211.55.9:5432/postgres"


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


# ----------------------------
# Schema utilities
# ----------------------------
def ensure_schema(engine, schema_name: str):
    """Ensure the target schema exists; create if missing."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :s"),
            {"s": schema_name}
        )
        if result.fetchone():
            log(f"Schema '{schema_name}' exists.", logging.INFO)
        else:
            log(f"Schema '{schema_name}' does not exist. Creating...", logging.WARNING)
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
            conn.commit()
            log(f"Schema '{schema_name}' created.", logging.INFO)


# ----------------------------
# Model factory (dynamic schema)
# ----------------------------
def make_models(schema: str):
    Base = declarative_base()

    class DescriptiveData(Base):
        __tablename__ = "descriptive_data"
        __table_args__ = {"schema": schema}

        measurement_id = Column(PG_UUID(as_uuid=True), default=uuid.uuid4, primary_key=True)
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
        project = Column(Text)
        template = Column(Text)
        parsed_date = Column(Text)

        def __repr__(self):
            return f"<DescriptiveData(measurement_id={self.measurement_id}, originator={self.originator}, ...)>"

    class MeasurementData(Base):
        __tablename__ = "measurement_data"
        __table_args__ = {"schema": schema}

        measurement_id = Column(PG_UUID(as_uuid=True), ForeignKey(f'{schema}.descriptive_data.measurement_id'), primary_key=True)
        sample_id = Column(Integer, primary_key=True)

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

        # Spectral columns (380–780nm step 10)
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
            return f"<MeasurementData(measurement_id={self.measurement_id}, sample_id={self.sample_id}, ...)>"

    return Base, DescriptiveData, MeasurementData


# ----------------------------
# Mapping helper
# ----------------------------
def build_descriptive_kwargs(desc_json: dict, model_cls, guid: uuid.UUID):
    """
    Build kwargs for SQLAlchemy model from JSON:
    - Only include keys that exist as columns on the model.
    - Ignore model fields missing from JSON.
    - Ignore extra JSON keys not in the model.
    - Light type coercion for Integer/DateTime.
    - JSON keys expected UPPER_SNAKE_CASE; model columns lower_snake_case.
    """
    cols = {c.name: c.type for c in model_cls.__table__.columns}
    out = {}
    for k, v in (desc_json or {}).items():
        col = k.lower()
        if col not in cols:
            continue
        if v is None or v == "":
            continue
        t = cols[col]
        try:
            tname = type(t).__name__.lower()
            if "integer" in tname:
                out[col] = int(v)
            elif "datetime" in tname:
                # Accept ISO ("YYYY-MM-DD" or with time)
                try:
                    out[col] = datetime.fromisoformat(v)
                except Exception:
                    # Try simple date-only
                    try:
                        out[col] = datetime.strptime(v, "%Y-%m-%d")
                    except Exception:
                        out[col] = v
            else:
                out[col] = v
        except Exception:
            out[col] = v
    if "measurement_id" in cols and "measurement_id" not in out and guid:
        out["measurement_id"] = guid
    return out


# ----------------------------
# Core
# ----------------------------
def process_file(json_file: Path, guid: uuid.UUID, db_uri: str, dry_run: bool = False, no_db_check: bool = False, schema: str = DEFAULT_SCHEMA):
    engine = create_engine(db_uri, echo=False)

    # Ensure schema exists (auto-create)
    ensure_schema(engine, schema)

    # Build models bound to this schema
    Base, DescriptiveData, MeasurementData = make_models(schema)

    # Create tables if missing
    Base.metadata.create_all(engine)
    # Ensure new columns exist (idempotent for existing DBs)
    with engine.connect() as conn:
        conn.execute(text(f'ALTER TABLE "{schema}".descriptive_data ADD COLUMN IF NOT EXISTS project TEXT'))
        conn.execute(text(f'ALTER TABLE "{schema}".descriptive_data ADD COLUMN IF NOT EXISTS template TEXT'))
        conn.execute(text(f'ALTER TABLE "{schema}".descriptive_data ADD COLUMN IF NOT EXISTS parsed_date TEXT'))
        conn.commit()
    log("Schema up-to-date: ensured project, template, parsed_date columns.", logging.INFO)
    # Create indexes for performance
    with engine.connect() as conn:
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_measurement_sample ON "{schema}".measurement_data (measurement_id, sample_id)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_measurement_id ON "{schema}".measurement_data (measurement_id)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_sample_id ON "{schema}".measurement_data (sample_id)'))
        conn.commit()
    log("Indexes ensured on measurement_data table.", logging.INFO)

    Session = sessionmaker(bind=engine)
    session = Session()

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    desc = data.get("descriptive_data", {}) or {}
    measurements = data.get("measurement_data", []) or []

    # Prefer MEASUREMENT_ID from JSON if present; otherwise use CLI/generated GUID
    json_guid = _extract_measurement_id_from_json(desc, measurements)
    guid_final = json_guid if json_guid is not None else guid
    log(f"Using measurement_id: {guid_final}", logging.INFO)

    # Force the SAME guid into each measurement row and basic validation
    cleaned_measurements = []
    for row in measurements:
        row["MEASUREMENT_ID"] = guid_final  # keep as uuid.UUID consistently
        if not row.get("MEASUREMENT_ID"):
            raise ValueError(f"Missing MEASUREMENT_ID in row: {row}")
        if "SAMPLE_ID" not in row:
            raise ValueError(f"Missing SAMPLE_ID in row: {row}")
        cleaned_measurements.append(row)

    # Duplicate (measurement_id, sample_id) guard
    pairs = [(r["MEASUREMENT_ID"], r["SAMPLE_ID"]) for r in cleaned_measurements]
    dupes = [k for k, v in Counter(pairs).items() if v > 1]
    if dupes:
        raise ValueError(f"Duplicate (measurement_id, sample_id) pairs found: {dupes}")

    # DescriptiveData: filtered kwargs (ignore missing + ignore extra) with SAME guid
    descriptive_entry = DescriptiveData(**build_descriptive_kwargs(desc, DescriptiveData, guid_final))

    # Dry-run: print sample + optional DB connectivity check, then exit
    if dry_run:
        log('Dry-run mode: Skipping all database inserts.', logging.INFO)
        # Show sample data
        import json as _json
        print("\n--- DRY RUN SAMPLE OUTPUT ---")
        print("Schema:", schema)
        print("DescriptiveData:")
        print(_json.dumps(build_descriptive_kwargs(desc, DescriptiveData, guid), indent=4, default=str))
        if cleaned_measurements:
            print("\nFirst 3 MeasurementData rows:")
            for sample_row in cleaned_measurements[:3]:
                print(_json.dumps(sample_row, indent=4, default=str))

        # Check DB connectivity if not disabled
        if not no_db_check:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print(f"DB connectivity: OK -> {engine.url}")
            except Exception as e:
                print(f"DB connectivity: FAILED -> {engine.url}\n{e}")
        
        print("--- END SAMPLE OUTPUT ---\n")
        print("\nPlanned index statements:")
        print(f'CREATE INDEX IF NOT EXISTS idx_measurement_sample ON "{schema}".measurement_data (measurement_id, sample_id)')
        print(f'CREATE INDEX IF NOT EXISTS idx_measurement_id ON "{schema}".measurement_data (measurement_id)')
        print(f'CREATE INDEX IF NOT EXISTS idx_sample_id ON "{schema}".measurement_data (sample_id)\n')
    
        session.close()
        return

    # --- Insert section ---
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
            "measurement_id": guid_final,  # enforce same UUID
            "sample_id": int(row["SAMPLE_ID"]),
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


# ----------------------------
# CLI
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Load measurement JSON into Postgres.")
    parser.add_argument("--path", default="/Users/aps/Docs/TestData/", help="Folder path containing JSON file")
    parser.add_argument("--json_file", default="printerTest.output.json", help="JSON file name")
    parser.add_argument("--measurement-id", dest="measurement_id", help="UUID for measurement_id (if not set, a new UUIDv4 will be generated)")
    parser.add_argument("--db", dest="db_uri", help="Override database connection URI")
    parser.add_argument("--dry-run", action="store_true", help="Validate and log but do not insert into database")
    parser.add_argument("--no-db-check", action="store_true", help="Skip DB connectivity check during --dry-run")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target schema name (auto-created if missing)")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    setup_logging(LOG_FILE, debug=args.debug, verbose=args.verbose)

    json_file = Path(args.path) / args.json_file
    if not json_file.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file}")

    # Determine measurement_id
    if args.measurement_id:
        try:
            guid = uuid.UUID(args.measurement_id)
        except ValueError:
            raise ValueError(f"Invalid UUID format: {args.measurement_id}")
    else:
        guid = uuid.uuid4()

    db_uri = args.db_uri or DB_URI
    print(f"Using DB URI: {db_uri}")
    schema = args.schema or DEFAULT_SCHEMA  # if empty string passed, fallback

    log(f"Using measurement_id: {guid}", logging.INFO)
    log(f"Schema: {schema}", logging.INFO)
    log(f"Processing file: {json_file}", logging.INFO)
    log(f"Effective measurement_id (JSON preferred): {guid}", logging.INFO)

    process_file(
        json_file=json_file,
        guid=guid,
        db_uri=db_uri,
        dry_run=args.dry_run,
        no_db_check=args.no_db_check,
        schema=schema,
    )

    log("✅ All measurement records inserted successfully.", logging.INFO)


if __name__ == "__main__":
    main()
