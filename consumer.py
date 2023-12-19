from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, ForeignKey, inspect, exists, text
from sqlalchemy.orm import declarative_base, Session, relationship, RelationshipProperty, class_mapper, ColumnProperty
import sys

DB_NAME = "NewOpenDataZNO2021"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create an SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Base.metadata.create_all(bind=engine)
session = Session(bind=engine)

# Create an inspector
inspector = inspect(engine)


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
    participant_id = Column(Integer, ForeignKey('participants.id'))
    test_center_id = Column(Integer, ForeignKey('history_test_centers.id'))
    HistTest = Column(String)
    HistLang = Column(String)
    HistTestStatus = Column(String)
    HistBall100 = Column(String)
    HistBall12 = Column(String)
    HistBall = Column(String)

    # Define relationships
    participant = relationship("Participants")
    test_center = relationship("HistoryTestCenters")


# Function to perform parameterized query
def parameterized_query(table_class, **kwargs):
    conditions = ' AND '.join(f"\"{field}\"='{value}'" for field, value in kwargs.items() if value)

    if len(conditions) == 0:
        query = text(f'''
            SELECT * FROM "{table_class.__tablename__}" ORDER BY "id" ASC
        ''')
    else:
        query = text(f'''
            SELECT * FROM "{table_class.__tablename__}"
            WHERE {conditions}
        ''')

    result = session.execute(query)
    records = result.fetchall()
    return records


def validate(table_class, data):
    # Validation for Participants table
    if table_class == Participants:
        # Check if OUTID is unique
        existing_record = session.query(Participants).filter_by(OUTID=data['OUTID']).first()
        if existing_record:
            print(f"Error: OUTID '{data['OUTID']}' is not unique in Participants table.")
            return False

        # Validate foreign keys
        for key, referenced_table in [('reg_location_id', RegLocation),
                                      ('edu_institution_id', EducationInstitution)]:
            key_value = data[key]

            if not key_value.isdecimal():
                print(f"Error: {key} must be an integer value.")
                return False

            # Query the referenced table
            if not session.query(referenced_table).filter_by(id=key_value).first():
                print(
                    f"Error: Foreign key validation failed for {key}={key_value}. No record found in {referenced_table.__tablename__}.")
                return False

    # Validation for HistoryTestResults table
    elif table_class == HistoryTestResults:

        # Validate foreign keys
        for key, referenced_table in [('participant_id', Participants),
                                      ('test_center_id', HistoryTestCenters)]:
            key_value = data[key]

            if not key_value.isdecimal():
                print(f"Error: {key} must be an integer value.")
                return False

            # Query the referenced table
            if not session.query(referenced_table).filter_by(id=key_value).first():
                print(
                    f"Error: Foreign key validation failed for {key}={key_value}. No record found in {referenced_table.__tablename__}.")
                return False

    return True

# Function to add data to a table
def add_data(table_class, data):
    try:
        if not validate(table_class, data):
            return
        new_record = table_class(**data)
        session.add(new_record)
        session.commit()
        print(f"Record added to {table_class.__tablename__} successfully.")
    except Exception as e:
        print(f"Error adding record to {table_class.__tablename__}: {e}")
        session.rollback()


# Function to delete records from a table
def delete_data(table_class, key_column, key_value):
    record_to_delete = session.query(table_class).filter_by(**{key_column: key_value}).first()
    if record_to_delete:
        session.delete(record_to_delete)
        session.commit()
        print(f"Record with {key_column}={key_value} deleted from {table_class.__tablename__} successfully.")
    else:
        print(f"No record found with {key_column}={key_value} in {table_class.__tablename__}.")


def print_query(table_class):
    # Extract column names
    columns = inspector.get_columns(table_class.__tablename__)
    fields = [column['name'] for column in columns]
    conditions = {field: input(f"Enter {field}: ") for field in fields}
    result = parameterized_query(table_class, **conditions)

    print("\n---------------------------------------------------------\n")
    print(f"Records in table \"{table_class.__tablename__}\" with conditions:")
    for field, value in conditions.items():
        print(f"{field}: {value}")

    print("\nResult:")

    if result:
        print(f"Found {len(result)} rows")

        print(fields)
        for record in range(0, len(result)):
            print(result[record])
    else:
        print("\nNo rows found.")


# Main console interface
def main():
    while True:
        menu_text = """
        *****************************************************
        *                   WELCOME TO MY MENU             *
        *****************************************************
        *  1. Parameterized Query                           *
        *  2. Add Data                                      *
        *  3. Delete Data                                   *
        *  4. Exit                                          *
        *****************************************************

        """
        print(menu_text)

        choice = input("Please enter the number corresponding to your choice: ")

        if choice == "1":
            menu_text = """
        *****************************************************
        *             PARAMETERIZED QUERY MENU              *
        *****************************************************
        *  1. Select Participant                            *
        *  2. Select RegLocation                            *
        *  3. Select EducationInstitution                   *
        *  4. Select HistoryTestCenters                     *
        *  5. Select HistoryTestResults                     *
        *****************************************************
        
        """
            print(menu_text)
            table_choice = input("Please enter the number corresponding to your choice: ")

            print("***TIP: LEAVE FIELD EMPTY AND PRESS ENTER IF FIELD IS NOT NECESSARY***")
            if table_choice == "1":
                # selecting data from Participants table
                print_query(Participants)

            elif table_choice == "2":
                # selecting data from RegLocation table
                print_query(RegLocation)

            elif table_choice == "3":
                # selecting data from EducationInstitution table
                print_query(EducationInstitution)

            elif table_choice == "4":
                # selecting data from HistoryTestCenters table
                print_query(HistoryTestCenters)

            elif table_choice == "5":
                # selecting data from HistoryTestResults table
                print_query(HistoryTestResults)

        elif choice == "2":
            menu_text = """
        *****************************************************
        *                   ADD DATA MENU                   *
        *****************************************************
        *  1. Add Participant                               *
        *  2. Add RegLocation                               *
        *  3. Add EducationInstitution                      *
        *  4. Add HistoryTestCenters                        *
        *  5. Add HistoryTestResults                        *
        *****************************************************
        
        """
            print(menu_text)
            table_choice = input("Please enter the number corresponding to your choice: ")

            print("***TIP: FIELD WITH STAR SYMBOL (*) IS NECESSARILY***")
            if table_choice == "1":
                # adding data to Participants table
                participant_data = {
                    "OUTID": input("Enter OUTID(must be unique): "),
                    "Birth": input("Enter Birth date (YYYY): "),
                    "SexTypeName": input("Enter SexTypeName: "),
                    "RegTypeName": input("Enter RegTypeName: "),
                    "ClassProfileNAME": input("Enter ClassProfileNAME: "),
                    "ClassLangName": input("Enter ClassLangName: "),
                    "reg_location_id": input("*Enter reg location id: "),
                    "edu_institution_id": input("*Enter edu institution id: "),
                }
                add_data(Participants, participant_data)

            elif table_choice == "2":
                # adding data to RegLocation table
                reg_location_data = {
                    "RegName": input("Enter RegName: "),
                    "AREANAME": input("Enter AREANAME: "),
                    "TERNAME": input("Enter TERNAME: "),
                    "TerTypeName": input("Enter TerTypeName: "),
                }
                add_data(RegLocation, reg_location_data)

            elif table_choice == "3":
                # adding data to EducationInstitution table
                edu_institution_data = {
                    "EOName": input("Enter EOName: "),
                    "EOTypeName": input("Enter EOTypeName: "),
                    "EORegName": input("Enter EORegName: "),
                    "EOAreaName": input("Enter EOAreaName: "),
                    "EOTerName": input("Enter EOTerName: "),
                    "EOParent": input("Enter EOParent: "),
                }
                add_data(EducationInstitution, edu_institution_data)

            elif table_choice == "4":
                # adding data to HistoryTestCenters table
                history_test_centers_data = {
                    "HistPTName": input("Enter HistPTName: "),
                    "HistPTRegName": input("Enter HistPTRegName: "),
                    "HistPTAreaName": input("Enter HistPTAreaName: "),
                    "HistPTTerName": input("Enter HistPTTerName: "),
                }
                add_data(HistoryTestCenters, history_test_centers_data)

            elif table_choice == "5":
                # adding data to HistoryTestResults table
                history_test_results_data = {
                    "participant_id": input("*Enter participant id: "),
                    "test_center_id": input("*Enter test center id: "),
                    "HistTest": input("Enter HistTest: "),
                    "HistLang": input("Enter HistLang: "),
                    "HistTestStatus": input("Enter HistTestStatus: "),
                    "HistBall100": input("Enter HistBall100: "),
                    "HistBall12": input("Enter HistBall12: "),
                    "HistBall": input("Enter HistBall: "),
                }
                add_data(HistoryTestResults, history_test_results_data)

        elif choice == "3":
            menu_text = """
        *****************************************************
        *             DELETE DATA MENU                      *
        *****************************************************
        *  1. Delete Participant                            *
        *  2. Delete RegLocation                            *
        *  3. Delete EducationInstitution                   *
        *  4. Delete HistoryTestCenters                     *
        *  5. Delete HistoryTestResults                     *
        *****************************************************

        """
            print(menu_text)
            table_choice = input("Please enter the number corresponding to your choice: ")

            if table_choice == "1":
                # deleting data from Participants table
                id_to_delete = input("Enter ID to delete: ")
                delete_data(Participants, "id", id_to_delete)

            elif table_choice == "2":
                # deleting data from RegLocation table
                id_to_delete = input("Enter ID to delete: ")
                delete_data(RegLocation, "id", id_to_delete)

            elif table_choice == "3":
                # deleting data from EducationInstitution table
                id_to_delete = input("Enter ID to delete: ")
                delete_data(EducationInstitution, "id", id_to_delete)

            elif table_choice == "4":
                # deleting data from HistoryTestCenters table
                id_to_delete = input("Enter ID to delete: ")
                delete_data(HistoryTestCenters, "id", id_to_delete)

            elif table_choice == "5":
                # deleting data from HistoryTestResults table
                id_to_delete = input("Enter ID to delete: ")
                delete_data(HistoryTestResults, "id", id_to_delete)

        elif choice == "4":
            print("Exiting the program.")
            sys.exit()

        else:
            print("Invalid choice. Please enter a valid option.")


if __name__ == '__main__':
    main()
