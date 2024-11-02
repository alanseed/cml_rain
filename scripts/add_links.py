"""
Application to enter the meta data for each link in the network

"""
import tkinter as tk
from tkinter import messagebox
from pymongo import MongoClient
import json


def clear_placeholder(event):
    current_text = coords_text.get("1.0", "end-1c")
    if current_text == "[ [lon1, lat1], [lon2, lat2] ]":
        coords_text.delete("1.0", "end")


# MongoDB connection (Local)
client = MongoClient('mongodb://localhost:27017/')
db = client['microwave_links']
collection = db['links']

# Function to insert data


def insert_data():
    link_id = link_id_entry.get()
    sublink_id = sublink_id_entry.get()
    radome = radome_var.get()
    frequency_value = freq_value_entry.get()
    frequency_units = freq_units_entry.get()

    # Get coordinates as a string
    coordinates = coords_text.get("1.0", "end-1c")

    # Parse coordinates input (comma-separated pairs)
    try:
        coordinates = json.loads(coordinates)
    except Exception as e:
        messagebox.showerror('Error', f'Invalid coordinates format: {e}')
        return

    # Collect MIBs and OIDs (comma-separated)
    mibs = mibs_entry.get().split(',')
    oids = oids_entry.get().split(',')

    # Check if the link_id and sublink_id combination already exists in the database
    existing_link = collection.find_one(
        {"properties.link_id": link_id, "properties.sublink_id": sublink_id})

    if existing_link:
        messagebox.showerror(
            'Error', 'This Link ID and Sublink ID combination already exists in the database.')
        return

    # Create the geoJSON structure
    geojson_data = {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates
        },
        "properties": {
            "link_id": link_id,
            "sublink_id": sublink_id,
            "radome": radome,
            "frequency": {
                "value": float(frequency_value),
                "units": frequency_units
            },
            "MIB": mibs,
            "OID": oids
        }
    }

    try:
        # Insert into MongoDB
        collection.insert_one(geojson_data)
        messagebox.showinfo('Success', 'Link data inserted successfully!')
    except Exception as e:
        messagebox.showerror('Error', f'Failed to insert data: {e}')


# GUI setup
root = tk.Tk()
root.title("Microwave Link Data Entry")

# Link ID
tk.Label(root, text="Link ID").grid(row=0, column=0)
link_id_entry = tk.Entry(root)
link_id_entry.grid(row=0, column=1)

# Sublink ID
tk.Label(root, text="Sublink ID").grid(row=1, column=0)
sublink_id_entry = tk.Entry(root)
sublink_id_entry.grid(row=1, column=1)

# Radome (Yes/No)
tk.Label(root, text="Radome").grid(row=2, column=0)
radome_var = tk.StringVar(value="no")
radome_menu = tk.OptionMenu(root, radome_var, "yes", "no")
radome_menu.grid(row=2, column=1)

# Frequency
tk.Label(root, text="Frequency Value").grid(row=3, column=0)
freq_value_entry = tk.Entry(root)
freq_value_entry.grid(row=3, column=1)

tk.Label(root, text="Frequency Units").grid(row=3, column=2)
freq_units_entry = tk.Entry(root)
freq_units_entry.grid(row=3, column=3)
freq_units_entry.insert(index=1,string="GHz")

# Coordinates (with placeholder text)
tk.Label(root, text="Coordinates (JSON)").grid(row=4, column=0)
coords_text = tk.Text(root, height=5, width=40)
coords_text.grid(row=4, column=1, columnspan=3)

# Insert the hint into the text box
coords_text.insert("1.0", "[ [lon1, lat1], [lon2, lat2] ]")
tk.Label(root, text="Coordinates (JSON)").grid(row=4, column=0)

# Bind the Text widget to clear the placeholder on click
coords_text.bind("<FocusIn>", clear_placeholder)

# MIBs
tk.Label(root, text="MIBs (comma-separated)").grid(row=5, column=0)
mibs_entry = tk.Entry(root)
mibs_entry.grid(row=5, column=1)

# OIDs
tk.Label(root, text="OIDs (comma-separated)").grid(row=6, column=0)
oids_entry = tk.Entry(root)
oids_entry.grid(row=6, column=1)

# Submit Button
submit_btn = tk.Button(root, text="Submit", command=insert_data)
submit_btn.grid(row=7, columnspan=4)

root.mainloop()
