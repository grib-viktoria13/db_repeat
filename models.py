# Step 1 Install Alembic
# -pip install alembic - for migration
#
# Step 2 Initialize Alembic
# -alembic init alembic - This will create an "alembic" directory with necessary configuration files.
#
# Step 3 Modify Alembic Configuration
# Edit the alembic.ini file to point to your database and SQLAlchemy model:
# -sqlalchemy.url = postgresql://postgres:123@localhost/NewOpenDataZNO2021
# Edit the env.py file for --autogenerate migration accessibility from models with following code:
# from models import Base
# target_metadata = Base.metadata
#


# After defining models.py
# Step 4 Create an Initial Migration
# -alembic revision --autogenerate -m "Initial migration"
#
# Step 5 Apply the Migration
# -alembic upgrade head

from sqlalchemy import create_engine, Column, String, Integer, Text, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

# Define the data model using SQLAlchemy ORM
Base = declarative_base()


class Participants(Base):
    __tablename__ = 'participants'

    id = Column(Integer, primary_key=True, autoincrement=True)
    OUTID = Column(String, unique=True)
    Birth = Column(String)
    SexTypeName = Column(String)
    RegTypeName = Column(String)
    ClassProfileNAME = Column(String)
    ClassLangName = Column(String)
    reg_location_id = Column(Integer, ForeignKey('register_locations.id'))
    edu_institution_id = Column(Integer, ForeignKey('education_institutions.id'))

    # Define relationships
    reg_location = relationship("RegLocation")
    edu_institution = relationship("EducationInstitution")


class RegLocation(Base):
    __tablename__ = 'register_locations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    RegName = Column(String)
    AREANAME = Column(String)
    TERNAME = Column(String)
    TerTypeName = Column(String)


class EducationInstitution(Base):
    __tablename__ = 'education_institutions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    EOName = Column(String)
    EOTypeName = Column(String)
    EORegName = Column(String)
    EOAreaName = Column(String)
    EOTerName = Column(String)
    EOParent = Column(String)


class HistoryTestCenters(Base):
    __tablename__ = 'history_test_centers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    HistPTName = Column(String)
    HistPTRegName = Column(String)
    HistPTAreaName = Column(String)
    HistPTTerName = Column(String)


class HistoryTestResults(Base):
    __tablename__ = 'history_test_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    OUTID = Column(String, ForeignKey('participants.OUTID'))
    test_center_id = Column(Integer, ForeignKey('history_test_centers.id'))
    HistTest = Column(String)
    HistLang = Column(String)
    HistTestStatus = Column(String)
    HistBall100 = Column(String)
    HistBall12 = Column(String)
    HistBall = Column(String)

    # Define relationships
    OUT = relationship("Participants")
    test_center = relationship("HistoryTestCenters")


class HistoryOld(Base):
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


# Define old and new database URLs
DATABASE_URL_OLD = "postgresql://postgres:123@localhost/OpenDataZNO2021"
DATABASE_URL_NEW = "postgresql://postgres:123@localhost/NewOpenDataZNO2021"

# Create engines for both databases
engine_old = create_engine(DATABASE_URL_OLD)
engine_new = create_engine(DATABASE_URL_NEW)

# Create sessions for both databases
SessionOld = sessionmaker(bind=engine_old)
SessionNew = sessionmaker(bind=engine_new)

session_old = SessionOld()
session_new = SessionNew()

# Step 1: Fill RegLocation Table without repeating, only unique values
unique_reg_locations = set()

for reg_location_old in session_old.query(HistoryOld).all():
    reg_location_key = (
        reg_location_old.RegName,
        reg_location_old.AREANAME,
        reg_location_old.TERNAME,
        reg_location_old.TerTypeName
    )

    if reg_location_key not in unique_reg_locations:
        unique_reg_locations.add(reg_location_key)

        reg_location_new = RegLocation(
            RegName=reg_location_old.RegName,
            AREANAME=reg_location_old.AREANAME,
            TERNAME=reg_location_old.TERNAME,
            TerTypeName=reg_location_old.TerTypeName
        )

        # Add the unique record to the new session
        session_new.add(reg_location_new)


# Commit the unique records to the new session
session_new.commit()
print("Reg Location Table filled Successfully!")

# Step 2: Fill EducationInstitution Table without repeating, only unique values
unique_education_institutions = set()

for edu_institution_old in session_old.query(HistoryOld).all():
    edu_institution_key = (
        edu_institution_old.EONAME,
        edu_institution_old.EOTypeName,
        edu_institution_old.EORegName,
        edu_institution_old.EOAreaName,
        edu_institution_old.EOTerName,
        edu_institution_old.EOParent
    )

    if edu_institution_key not in unique_education_institutions:
        unique_education_institutions.add(edu_institution_key)

        edu_institution_new = EducationInstitution(
            EOName=edu_institution_old.EONAME,
            EOTypeName=edu_institution_old.EOTypeName,
            EORegName=edu_institution_old.EORegName,
            EOAreaName=edu_institution_old.EOAreaName,
            EOTerName=edu_institution_old.EOTerName,
            EOParent=edu_institution_old.EOParent
        )

        # Add the unique record to the new session
        session_new.add(edu_institution_new)


# Commit the unique records to the new session
session_new.commit()
print("Education Institution Table filled Successfully!")

# Step 3: Fill HistoryTestCenters Table without repeating, only unique values
unique_history_test_centers = set()

for history_test_center_old in session_old.query(HistoryOld).all():
    history_test_center_key = (
        history_test_center_old.HistPTName,
        history_test_center_old.HistPTRegName,
        history_test_center_old.HistPTAreaName,
        history_test_center_old.HistPTTerName
    )

    if history_test_center_key not in unique_history_test_centers:
        unique_history_test_centers.add(history_test_center_key)

        history_test_center_new = HistoryTestCenters(
            HistPTName=history_test_center_old.HistPTName,
            HistPTRegName=history_test_center_old.HistPTRegName,
            HistPTAreaName=history_test_center_old.HistPTAreaName,
            HistPTTerName=history_test_center_old.HistPTTerName
        )

        # Add the unique record to the new session
        session_new.add(history_test_center_new)


# Commit the unique records to the new session
session_new.commit()
print("History Test Center Table filled Successfully!")

# Step 4: Fill Participants Table and fill by id fields: reg_location_id (Foreign Key), edu_institution_id (Foreign Key)
for participant_old in session_old.query(HistoryOld).all():
    print(participant_old.id)

    if participant_old.id == 14000:
        break

    reg_location_new = session_new.query(RegLocation).filter_by(
        RegName=participant_old.RegName,
        AREANAME=participant_old.AREANAME,
        TERNAME=participant_old.TERNAME,
        TerTypeName=participant_old.TerTypeName).first()

    edu_institution_new = session_new.query(EducationInstitution).filter_by(
        EOName=participant_old.EONAME,
        EOTypeName=participant_old.EOTypeName,
        EORegName=participant_old.EORegName,
        EOAreaName=participant_old.EOAreaName,
        EOTerName=participant_old.EOTerName,
        EOParent=participant_old.EOParent).first()

    if reg_location_new and edu_institution_new:
        participant_new = Participants(
            OUTID=participant_old.OUTID,
            Birth=participant_old.Birth,
            SexTypeName=participant_old.SexTypeName,
            RegTypeName=participant_old.RegTypeName,
            ClassProfileNAME=participant_old.ClassProfileNAME,
            ClassLangName=participant_old.ClassLangName,
            reg_location_id=reg_location_new.id,
            edu_institution_id=edu_institution_new.id
        )

        session_new.add(participant_new)


# Commit the changes to the new session
session_new.commit()
print("Participants Table filled Successfully!")

# Step 5: Fill history_test_results Table and fill by id fields: OUTID (Foreign Key), test_center_id (Foreign Key)
for history_test_result_old in session_old.query(HistoryOld).all():
    print(history_test_result_old.id)

    if history_test_result_old.id == 14000:
        break

    participant_new = session_new.query(Participants).filter_by(OUTID=history_test_result_old.OUTID).first()
    test_center_new = session_new.query(HistoryTestCenters).filter_by(
        HistPTName=history_test_result_old.HistPTName,
        HistPTRegName=history_test_result_old.HistPTRegName,
        HistPTAreaName=history_test_result_old.HistPTAreaName,
        HistPTTerName=history_test_result_old.HistPTTerName
    ).first()

    if participant_new and test_center_new:
        history_test_result_new = HistoryTestResults(
            OUTID=participant_new.OUTID,
            test_center_id=test_center_new.id,
            HistTest=history_test_result_old.HistTest,
            HistLang=history_test_result_old.HistLang,
            HistTestStatus=history_test_result_old.HistTestStatus,
            HistBall100=history_test_result_old.HistBall100,
            HistBall12=history_test_result_old.HistBall12,
            HistBall=history_test_result_old.HistBall
        )
        session_new.add(history_test_result_new)

# Commit the changes to the new session
session_new.commit()
print("History Test Results Table filled Successfully!")


# Close sessions
session_old.close()
session_new.close()
