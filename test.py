
# import csv
# import random
# from datetime import datetime, timedelta

# def random_datetime(start, end):
#     return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

# vehicle_ids = [f"V{str(i).zfill(5)}" for i in range(1, 1001)]
# zone_ids = ["Z001", "Z002", "Z003", "Z004", "Z005"]
# rate_map = {"Z001": 50, "Z002": 40, "Z003": 30, "Z004": 70, "Z005": 25}
# start_date = datetime(2023, 1, 1)
# end_date = datetime(2024, 12, 31)

# with open("parking_events_large.csv", mode="w", newline="") as file:
#     writer = csv.writer(file)
#     writer.writerow(["event_id", "vehicle_id", "zone_id", "entry_time", "exit_time", "paid_amount"])

#     for i in range(1, 200):
#         vehicle_id = random.choice(vehicle_ids)
#         zone_id = random.choice(zone_ids)
#         entry_time = random_datetime(start_date, end_date)
#         duration = timedelta(minutes=random.randint(15, 360))
#         exit_time = entry_time + duration
#         rate = rate_map[zone_id]
#         paid_amount = round(rate * (duration.total_seconds() / 3600), 2)

#         writer.writerow([
#             f"E{str(i).zfill(7)}",
#             vehicle_id,
#             zone_id,
#             entry_time.strftime("%Y-%m-%d %H:%M:%S"),
#             exit_time.strftime("%Y-%m-%d %H:%M:%S"),
#             paid_amount
#         ])

import pandas as pd

df = pd.read_parquet('data/parking_events.parquet')
print(df.head())