from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from pymongo import MongoClient


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
    reg_location_id = Column(Integer, ForeignKey('register_locations.id', ondelete="CASCADE"))
    edu_institution_id = Column(Integer, ForeignKey('education_institutions.id', ondelete="CASCADE"))

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
    participant_id = Column(Integer, ForeignKey('participants.id', ondelete="CASCADE"))
    test_center_id = Column(Integer, ForeignKey('history_test_centers.id', ondelete="CASCADE"))
    HistTest = Column(String)
    HistLang = Column(String)
    HistTestStatus = Column(String)
    HistBall100 = Column(String)
    HistBall12 = Column(String)
    HistBall = Column(String)

    # Define relationships
    participant = relationship("Participants")
    test_center = relationship("HistoryTestCenters")


# Define the PostgreSQL connection string
username = 'postgres'
password = '123'
database = 'NewOpenDataZNO2021'
host = 'localhost'

DATABASE_URL = f"postgresql://{username}:{password}@{host}/{database}"

# SQLAlchemy PostgreSQL engine
postgres_engine = create_engine(DATABASE_URL)

# SQLAlchemy session for PostgreSQL
Session = sessionmaker(bind=postgres_engine)
postgres_session = Session()

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['MongoNewOpenDataZNO2021']

# Define MongoDB collections
participants_collection = mongo_db['participants']
register_locations_collection = mongo_db['register_locations']
education_institutions_collection = mongo_db['education_institutions']
history_test_centers_collection = mongo_db['history_test_centers']
history_test_results_collection = mongo_db['history_test_results']


# Query PostgreSQL and migrate data to MongoDB
def migrate_data():
    # Step 1: Migrate RegisterLocations
    for reg_location_old in postgres_session.query(RegLocation):
        register_location_doc = {
            'RegName': reg_location_old.RegName,
            'AREANAME': reg_location_old.AREANAME,
            'TERNAME': reg_location_old.TERNAME,
            'TerTypeName': reg_location_old.TerTypeName
        }
        register_locations_collection.insert_one(register_location_doc)

    print("Reg Location Table created and filled Successfully!")

    # Step 2: Migrate EducationInstitutions
    for edu_institution_old in postgres_session.query(EducationInstitution):
        edu_institution_doc = {
            'EOName': edu_institution_old.EOName,
            'EOTypeName': edu_institution_old.EOTypeName,
            'EORegName': edu_institution_old.EORegName,
            'EOAreaName': edu_institution_old.EOAreaName,
            'EOTerName': edu_institution_old.EOTerName,
            'EOParent': edu_institution_old.EOParent
        }
        education_institutions_collection.insert_one(edu_institution_doc)

    print("Education Institution Table created and filled Successfully!")

    # Step 3: Migrate HistoryTestCenters
    for history_test_center_old in postgres_session.query(HistoryTestCenters):
        history_test_center_doc = {
            'HistPTName': history_test_center_old.HistPTName,
            'HistPTRegName': history_test_center_old.HistPTRegName,
            'HistPTAreaName': history_test_center_old.HistPTAreaName,
            'HistPTTerName': history_test_center_old.HistPTTerName
        }
        history_test_centers_collection.insert_one(history_test_center_doc)

    print("History Test Center Table created and filled Successfully!")

    # Step 4: Migrate Participants
    for participant_old in postgres_session.query(Participants):
        participant_doc = {
            'OUTID': participant_old.OUTID,
            'Birth': participant_old.Birth,
            'SexTypeName': participant_old.SexTypeName,
            'RegTypeName': participant_old.RegTypeName,
            'ClassProfileNAME': participant_old.ClassProfileNAME,
            'ClassLangName': participant_old.ClassLangName,
            'reg_location_id': participant_old.reg_location_id,
            'edu_institution_id': participant_old.edu_institution_id
        }
        participants_collection.insert_one(participant_doc)

    print("Participants Table created and filled Successfully!")

    # Step 5: Migrate HistoryTestResults
    for history_test_result_old in postgres_session.query(HistoryTestResults):
        history_test_result_doc = {
            'participant_id': history_test_result_old.participant_id,
            'test_center_id': history_test_result_old.test_center_id,
            'HistTest': history_test_result_old.HistTest,
            'HistLang': history_test_result_old.HistLang,
            'HistTestStatus': history_test_result_old.HistTestStatus,
            'HistBall100': history_test_result_old.HistBall100,
            'HistBall12': history_test_result_old.HistBall12,
            'HistBall': history_test_result_old.HistBall
        }
        history_test_results_collection.insert_one(history_test_result_doc)

    print("History Test Results Table created and filled Successfully!")


# Function for parameterized queries in MongoDB
def execute_mongo_query(collection, query_params):
    result = collection.find(query_params)
    return list(result)


# Function to convert specific fields to number if applicable
def convert_fields_to_number(query_params):
    fields_to_convert = ['participant_id', 'test_center_id', 'reg_location_id', 'edu_institution_id']
    for field in fields_to_convert:
        if field in query_params and query_params[field].isdigit():
            query_params[field] = int(query_params[field])
    return query_params


# Menu for migrate Data and parameterized queries
if __name__ == '__main__':
    while True:
        print("\nMenu:")
        print("1. Migrate Data (Funnel)")
        print("2. Parameterized Query")
        print("3. Exit")

        choice = input("Enter your choice (1, 2, or 3): ")

        if choice == '1':
            print("\nMigrating Data...")
            migrate_data()
            print("Data Migration Completed.")
        elif choice == '2':
            # Choose table for parameterized query
            print("\nChoose the MongoDB collection for the parameterized query:")
            print("1. Participants")
            print("2. Register Locations")
            print("3. Education Institutions")
            print("4. History Test Centers")
            print("5. History Test Results")

            table_choice = input("Enter your choice (1, 2, 3, 4, or 5): ")
            collection_to_query = None

            if table_choice == '1':
                collection_to_query = participants_collection
            elif table_choice == '2':
                collection_to_query = register_locations_collection
            elif table_choice == '3':
                collection_to_query = education_institutions_collection
            elif table_choice == '4':
                collection_to_query = history_test_centers_collection
            elif table_choice == '5':
                collection_to_query = history_test_results_collection
            else:
                print("Invalid choice for the table.")
                continue

            # Specify fields for the parameter
            query_param_name = input("Enter the parameter name: ")
            query_param_value = input("Enter the parameter value: ")

            # Create query parameters dictionary
            query_params = {query_param_name: query_param_value}

            # Convert specific fields to number
            query_params = convert_fields_to_number(query_params)

            if collection_to_query is not None:
                result = execute_mongo_query(collection_to_query, query_params)
                print("\nQuery Result:")
                if result:
                    for doc in result:
                        print(doc)
                else:
                    print("No matching documents found.")
            else:
                print("Invalid collection name.")
        elif choice == '3':
            print("Exiting the program.")
            mongo_client.close()
            postgres_session.close()
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
