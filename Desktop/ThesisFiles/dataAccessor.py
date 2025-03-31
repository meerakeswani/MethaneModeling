import ee
ee.Authenticate() 
ee.Initialize(project='carbide-digit-449206-r9')
# Initialize Earth Engine


# Define the region of interest: Roswell, New Mexico
roswell_region = ee.Geometry.Rectangle([-104.75, 33.25, -104.3, 33.5])  # Adjust if needed

# Function to create a grid of points (10km spacing)
def create_grid(region, spacing_km):
    spacing_m = spacing_km * 1000
    bounds = region.bounds().coordinates().get(0)
    coords = bounds.getInfo()
    
    min_lon, min_lat = coords[0]
    max_lon, max_lat = coords[2]

    lon_points = int((max_lon - min_lon) * 111 / spacing_km)
    lat_points = int((max_lat - min_lat) * 111 / spacing_km)

    points = []
    for i in range(lon_points):
        for j in range(lat_points):
            lon = min_lon + i * spacing_km / 111
            lat = min_lat + j * spacing_km / 111
            points.append(ee.Feature(ee.Geometry.Point(lon, lat)))

    return ee.FeatureCollection(points)

# Create a grid over Roswell with 10km spacing and a 7km buffer
roswell_grid = create_grid(roswell_region, spacing_km=10).map(lambda f: f.buffer(7000))

# Load the Sentinel-5P CH4 dataset with missing values handled
ch4_collection = ee.ImageCollection('COPERNICUS/S5P/OFFL/L3_CH4') \
    .select('CH4_column_volume_mixing_ratio_dry_air') \
    .map(lambda img: img.unmask(-9999))  # Fill missing values with -9999

# Filter for the full date range: April 2018 to December 2021
full_dates = ch4_collection.filterDate('2018-04-01', '2021-12-31')

# Function to extract CH4 data for each date
def extract_ch4(image):
    date = image.date().format('YYYY-MM-dd')
    reduced = image.reduceRegions(
        collection=roswell_grid,
        reducer=ee.Reducer.mean(),
        scale=10000  # Match Sentinel-5P resolution (~10 km per pixel)
    )
    return reduced.map(lambda f: f.set({
        'date': date,
        'CH4_value': f.get('CH4_column_volume_mixing_ratio_dry_air')  # Ensure property is set
    }))

# Apply extraction over the entire date range
full_data = full_dates.map(extract_ch4).flatten()

# Remove completely empty values (-9999 means no data)
filtered_full_data = full_data.filter(ee.Filter.neq('CH4_value', -9999))

# Export the full dataset to Google Drive
task = ee.batch.Export.table.toDrive(
    collection=filtered_full_data,
    description='CH4_Roswell_2018_2021',
    fileFormat='CSV'
)
task.start()
