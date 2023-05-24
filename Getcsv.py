import pandas as pd
import os
import shutil
import csv

path = "C:/Users/E80-1798/Desktop/AMZ Payment"
final = "C:/Users/E80-1798/Desktop/Payment Final"

for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith(".csv"):
            source_path = os.path.join(root, file)
            dest_path = os.path.join(final, file)
            shutil.move(source_path, dest_path)
