import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String, Column, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base

# Database connection
engine = create_engine('sqlite:///climate_data_warehouse.db', echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# Dimensions
class YearDim(Base):
    __tablename__ = 'year_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, unique=True, nullable=False)

class TemperatureCategoryDim(Base):
    __tablename__ = 'temperature_category_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String, unique=True, nullable=False)

class CO2CategoryDim(Base):
    __tablename__ = 'co2_category_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String, unique=True, nullable=False)

class SeaLevelCategoryDim(Base):
    __tablename__ = 'sea_level_category_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String, unique=True, nullable=False)

class ExtremeEventTypeDim(Base):
    __tablename__ = 'extreme_event_type_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String, unique=True, nullable=False)

class RegionDim(Base):
    __tablename__ = 'region_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    region = Column(String, unique=True, nullable=False)

class EcosystemDim(Base):
    __tablename__ = 'ecosystem_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ecosystem = Column(String, unique=True, nullable=False)

class SourceDim(Base):
    __tablename__ = 'source_dim'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, unique=True, nullable=False)

# Fact Tables
class ClimateIndicatorsFact(Base):
    __tablename__ = 'climate_indicators_fact'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year_id = Column(Integer, ForeignKey('year_dim.id'), nullable=False)
    temp_category_id = Column(Integer, ForeignKey('temperature_category_dim.id'), nullable=False)
    co2_category_id = Column(Integer, ForeignKey('co2_category_dim.id'), nullable=False)
    sea_level_category_id = Column(Integer, ForeignKey('sea_level_category_dim.id'), nullable=False)
    source_id = Column(Integer, ForeignKey('source_dim.id'), nullable=False)
    global_avg_temp = Column(Float, nullable=False)
    co2_concentration = Column(Float, nullable=False)
    sea_level_rise = Column(Float, nullable=False)

class ExtremeEventsFact(Base):
    __tablename__ = 'extreme_events_fact'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year_id = Column(Integer, ForeignKey('year_dim.id'), nullable=False)
    event_type_id = Column(Integer, ForeignKey('extreme_event_type_dim.id'), nullable=False)
    region_id = Column(Integer, ForeignKey('region_dim.id'), nullable=False)
    ecosystem_id = Column(Integer, ForeignKey('ecosystem_dim.id'), nullable=False)
    source_id = Column(Integer, ForeignKey('source_dim.id'), nullable=False)
    extreme_events_count = Column(Integer, nullable=False)
    economic_loss = Column(Float, nullable=False)

# Create tables
Base.metadata.create_all(engine)

# Load data
csv_file = 'categorical_data_and_dimensions.csv'  
chunk_size = 100000 

# Functions to get or insert dimension data
def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance.id
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance.id

for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
    # Reemplazar valores nulos en "Extreme Event Type" por "Desconocido"
    chunk['Extreme Event Type'] = chunk['Extreme Event Type'].fillna("Desconocido")

    for _, row in chunk.iterrows():
        year_id = get_or_create(session, YearDim, year=row['Year'])
        temp_category_id = get_or_create(session, TemperatureCategoryDim, category=row['Temperature Category'])
        co2_category_id = get_or_create(session, CO2CategoryDim, category=row['CO2 Category'])
        sea_level_category_id = get_or_create(session, SeaLevelCategoryDim, category=row['Sea Level Category'])
        event_type_id = get_or_create(session, ExtremeEventTypeDim, event_type=row['Extreme Event Type'])
        region_id = get_or_create(session, RegionDim, region=row['Region'])
        ecosystem_id = get_or_create(session, EcosystemDim, ecosystem=row['Ecosystem'])
        source_id = get_or_create(session, SourceDim, source=row['Data Source'])

        # Insert into fact tables
        climate_fact = ClimateIndicatorsFact(
            year_id=year_id,
            temp_category_id=temp_category_id,
            co2_category_id=co2_category_id,
            sea_level_category_id=sea_level_category_id,
            source_id=source_id,
            global_avg_temp=row['Global Average Temperature (°C)'],
            co2_concentration=row['CO2 Concentration_ppm'],
            sea_level_rise=row['Sea Level Rise_mm']
        )
        session.add(climate_fact)

        extreme_fact = ExtremeEventsFact(
            year_id=year_id,
            event_type_id=event_type_id,
            region_id=region_id,
            ecosystem_id=ecosystem_id,
            source_id=source_id,
            extreme_events_count=row['Extreme Events Count'],
            economic_loss=row['Economic Loss (billions)']
        )
        session.add(extreme_fact)

    session.commit()
session.close()
