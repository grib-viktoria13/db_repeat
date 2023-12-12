# Install the required libraries
# pip install sqlalchemy psycopg2
import logging

from sqlalchemy import create_engine, Column, String, Integer, Text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv  # Assuming your unstructured data is in a CSV file

# Define the PostgreSQL connection string
username = 'postgres'
password = '123'
database = 'OpenDataZNO2021'
host = 'localhost'

DATABASE_URL = f"postgresql://{username}:{password}@{host}/{database}"

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define the data model using SQLAlchemy ORM
Base = declarative_base()


class YourTable(Base):
    __tablename__ = 'History'

    id = Column(Integer, primary_key=True)
    OUTID = Column(String)
    Birth = Column(String)
    SexTypeName = Column(String)
    RegName = Column(String)
    AREANAME = Column(String)
    TERNAME = Column(String)
    RegTypeName = Column(String)
    TerTypeName = Column(String)
    ClassProfileNAME = Column(String)
    ClassLangName = Column(String)
    EONAME = Column(String)
    EOTypeName = Column(String)
    EORegName = Column(String)
    EOAreaName = Column(String)
    EOTerName = Column(String)
    EOParent = Column(String)
    HistTest = Column(String)
    HistLang = Column(String)
    HistTestStatus = Column(String)
    HistBall100 = Column(String)
    HistBall12 = Column(String)
    HistBall = Column(String)
    HistPTName = Column(String)
    HistPTRegName = Column(String)
    HistPTAreaName = Column(String)
    HistPTTerName = Column(String)


# # Create the table in the database
Base.metadata.create_all(bind=engine)
#
# # Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Read data from an external file (e.g., CSV) and insert it into the database
with open('C:/Users/vikto/PycharmProjects/pythonProject/venv/db_labs/Odata2021File.csv',
          'r', encoding="utf8") as file:
    reader = csv.DictReader(file, delimiter=';')

    for row in reader:
        try:
            data_entry = YourTable(
                OUTID=row['\ufeff"OUTID"'],
                Birth=row['Birth'],
                SexTypeName=row['SexTypeName'],
                RegName=row['RegName'],
                AREANAME=row['AREANAME'],
                TERNAME=row['TERNAME'],
                RegTypeName=row['RegTypeName'],
                TerTypeName=row['TerTypeName'],
                ClassProfileNAME=row['ClassProfileNAME'],
                ClassLangName=row['ClassLangName'],
                EONAME=row['EONAME'],
                EOTypeName=row['EOTypeName'],
                EORegName=row['EORegName'],
                EOAreaName=row['EOAreaName'],
                EOTerName=row['EOTerName'],
                EOParent=row['EOParent'],
                HistTest=row['HistTest'],
                HistLang=row['HistLang'],
                HistTestStatus=row['HistTestStatus'],
                HistBall100=row['HistBall100'],
                HistBall12=row['HistBall12'],
                HistBall=row['HistBall'],
                HistPTName=row['HistPTName'],
                HistPTRegName=row['HistPTRegName'],
                HistPTAreaName=row['HistPTAreaName'],
                HistPTTerName=row['HistPTTerName'],)

            # Add more columns as needed
            session.add(data_entry)
            print("ROW ADDED SUCCESSFULLY!")

        except Exception as e:
            logging.error(f"Error adding row: {e}")
            session.rollback()

# Commit the changes to the database
session.commit()
