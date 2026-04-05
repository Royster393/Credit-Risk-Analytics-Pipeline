import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

# --- CONFIGURATION ---
SERVER_NAME = r'LOCALHOST\SQLEXPRESS' 
DATABASE_NAME = 'FinancePortfolioDB'

# Relative pathing: Move up from /script/ to root, then into /csv/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_FILE = os.path.join(BASE_DIR, 'csv', 'credit_risk_dataset.csv')

def run_star_schema_pipeline():
    print(f"--- Deploying Star Schema Pipeline for {SERVER_NAME} ---")
    
    if not os.path.exists(LOCAL_FILE):
        print(f"ERROR: Cannot find file at: {LOCAL_FILE}")
        return

    # Coordinates for the Dimension Table
    branch_coords = {
        'Windsor':   {'Lat': 42.3149, 'Lon': -83.0364},
        'Toronto':   {'Lat': 43.6532, 'Lon': -79.3832},
        'Ottawa':    {'Lat': 45.4215, 'Lon': -75.6972},
        'London':    {'Lat': 42.9849, 'Lon': -81.2496},
        'Hamilton':  {'Lat': 43.2557, 'Lon': -79.8711},
        'Kitchener': {'Lat': 43.4516, 'Lon': -80.4925}
    }

    try:
        # 1. EXTRACT & CLEAN
        df = pd.read_csv(LOCAL_FILE)
        df = df[df['person_age'] < 95]
        df['person_emp_length'] = df['person_emp_length'].fillna(df['person_emp_length'].median())
        df['loan_int_rate'] = df['loan_int_rate'].fillna(df['loan_int_rate'].median())

        # 2. TRANSFORM
        stress_rate = 0.05 
        cities = list(branch_coords.keys())
        df['City'] = np.random.choice(cities, size=len(df))
        
        df['Stressed_DTI'] = (df['loan_amnt'] * (1 + stress_rate)) / df['person_income']
        df['Risk_Level'] = np.where(
            (df['loan_status'] == 1) | (df['Stressed_DTI'] > 0.40), 
            'High Risk', 
            'Standard'
        )

        # 3. LOAD
        engine = create_engine(f'mssql+pyodbc://@{SERVER_NAME}/{DATABASE_NAME}?driver=ODBC+Driver+17+for+SQL+Server')
        
        if 'id' not in df.columns:
            df['id'] = df.index

        # --- Table 1: Fact_Credit_Risk (The "What happened") ---
        # Notice we DON'T put Lat/Lon here anymore to keep it clean
        fact_cols = [
            'id', 'person_age', 'person_income', 'loan_amnt', 'loan_status', 'Risk_Level', 
            'City', 'Stressed_DTI', 'loan_grade', 'loan_intent', 
            'person_home_ownership', 'cb_person_default_on_file', 
            'cb_person_cred_hist_length', 'loan_int_rate'
        ]
        df[fact_cols].to_sql('Fact_Credit_Risk', engine, if_exists='replace', index=False)

        # --- Table 2: Dim_Locations (The "Where it happened") ---
        dim_location_data = {
            'City': list(branch_coords.keys()),
            'Province': ['Ontario'] * len(branch_coords),
            'Latitude': [v['Lat'] for v in branch_coords.values()],
            'Longitude': [v['Lon'] for v in branch_coords.values()]
        }
        pd.DataFrame(dim_location_data).to_sql('Dim_Locations', engine, if_exists='replace', index=False)
        
        print(f"SUCCESS: 2 Tables Created (Fact_Credit_Risk & Dim_Locations)")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    run_star_schema_pipeline()