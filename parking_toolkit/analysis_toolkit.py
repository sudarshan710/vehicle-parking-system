import os
import sys
import argparse
import duckdb

FLAG_PATH = 'flag.flag'

def top_performing_zone():
    print('\n The following are Top 5 Performing Zone in terms of parking amount paid: ')
    df = duckdb.query("""
                      SELECT zone_id, zone_name, sum(paid_amount) as total_amount
                      FROM 'final_joined_events.parquet'
                      WHERE paid_amount>0
                      GROUP BY zone_id, zone_name  
                      ORDER BY sum(paid_amount) desc                      
                      LIMIT 5
                  """).df()
    print(df)

def most_frequent_parkers():
    print('\n The following are Top 5 Frequent Parkers: ')
    df = duckdb.query("""
                      SELECT vehicle_id, owner_name, type, count(vehicle_id) as frequency
                      FROM 'final_joined_events.parquet'
                      GROUP BY vehicle_id, owner_name, type
                      ORDER BY count(vehicle_id) desc
                      LIMIT 5
                      """).df()
    print(df)

def compare_zone_perfor():
    print('\nComparative Zone Performance: Total Paid Amount vs Number of Parking Events\n')
    df = duckdb.query("""
        SELECT 
            zone_id, 
            zone_name, 
            COUNT(*) AS total_events,
            SUM(paid_amount) AS total_paid
        FROM 'final_joined_events.parquet'
        GROUP BY zone_id, zone_name
        ORDER BY total_paid DESC
        LIMIT 5
    """).df()
    print(df)

def main(analysis):
    if not os.path.exists(FLAG_PATH):
        print("\n Error: You must run CLI1 (ingestinon->writing to database) first to complete the setup.\n")
        sys.exit(1)
    for choice in analysis:
        if choice == 'top-zone':
            top_performing_zone()
        elif choice == 'freq-parkers':
            most_frequent_parkers()
        elif choice == 'comp-zone':
            compare_zone_perfor()
        else:
            print(f"\n Invalid choice\n")
            sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('analysis', nargs='+' ,choices=['top-zone', 'freq-parkers', 'comp-zone'], help='Choose your analysis type')

    args = parser.parse_args()

    main(args.analysis)
