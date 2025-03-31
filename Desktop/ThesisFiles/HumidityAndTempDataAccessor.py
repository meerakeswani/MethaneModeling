import ee

# Authenticate and initialize
ee.Authenticate()
ee.Initialize(project='carbide-digit-449206-r9')

# Define Roswell region
roswell_region = ee.Geometry.Rectangle([-104.75, 33.25, -104.3, 33.5])

# Create a 10km grid
def create_grid(region, spacing_km):
    spacing_deg = spacing_km / 111.0
    bounds = region.bounds().coordinates().get(0).getInfo()
    
    min_lon, min_lat = bounds[0]
    max_lon, max_lat = bounds[2]

    lon_points = int((max_lon - min_lon) / spacing_deg)
    lat_points = int((max_lat - min_lat) / spacing_deg)

    points = []
    for i in range(lon_points + 1):
        for j in range(lat_points + 1):
            lon = min_lon + i * spacing_deg
            lat = min_lat + j * spacing_deg
            points.append(ee.Feature(ee.Geometry.Point(lon, lat)))

    return ee.FeatureCollection(points)

roswell_grid = create_grid(roswell_region, spacing_km=10).map(lambda f: f.buffer(7000))

# 1️⃣ IMPORT the methane CSV as a FeatureCollection
methane_table = ee.FeatureCollection('projects/carbide-digit-449206-r9/assets/CH4_Roswell_2018_2021_clean2')
#
## 🟢 1A: Clean the date format to 'YYYY-MM-dd'
#def clean_date(feature):
#    raw_date = ee.String(feature.get('date'))
#    
#    # Check if raw_date contains '/' → non-ISO format like '9/14/21'
#    has_slash = raw_date.index('/').gt(-1)
#    
#    # If has slash, parse as 'M/d/yy', else assume ISO
#    parsed_date = ee.Algorithms.If(
#        has_slash,
#        ee.Date.parse('M/d/yy', raw_date),
#        ee.Date(raw_date)
#    )
#    
#    formatted_date = ee.Date(parsed_date).format('YYYY-MM-dd')
#    return feature.set('date', formatted_date)
#
## Apply date cleaning
#methane_table_clean = methane_table.map(clean_date)

# 2️⃣ FILTER dates where methane mean != -9999
valid_dates_fc = methane_table.filter(ee.Filter.neq('mean', -9999))

# 3️⃣ Extract valid dates as a list
valid_dates = valid_dates_fc.aggregate_array('date')

# 4️⃣ Load temperature dataset and filter by valid dates
temperatureCollection = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY') \
    .select('temperature_2m') \
    .map(lambda img: img.subtract(273.15).set('date', img.date().format('YYYY-MM-dd'))) \
    .filterDate('2018-04-01', '2019-04-01') \
    .filter(ee.Filter.inList('date', valid_dates))

# Aggregate hourly temperature to daily mean
def daily_mean_temperature(date):
    daily_images = temperatureCollection.filter(ee.Filter.date(date, ee.Date(date).advance(1, 'day')))
    daily_mean = daily_images.mean().set('date', date).rename('temperature_2m')
    return ee.Image(daily_mean)

date_range = valid_dates
dailyTemperature = ee.ImageCollection.fromImages(date_range.map(lambda d: daily_mean_temperature(ee.Date(d))))

# 5️⃣ Load humidity dataset, filtered by valid dates
humidityCollection = ee.ImageCollection('ECMWF/ERA5/DAILY') \
    .select('total_precipitable_water') \
    .map(lambda img: img.set('date', img.date().format('YYYY-MM-dd'))) \
    .filterDate('2018-04-01', '2019-04-01') \
    .filter(ee.Filter.inList('date', valid_dates))

# Function to extract temperature
def extract_temperature(image):
    date = image.get('date')
    reduced = image.reduceRegions(
        collection=roswell_grid,
        reducer=ee.Reducer.mean(),
        scale=9000
    )
    return reduced.map(lambda f: f.set({'date': date, 'Temperature_C': f.get('temperature_2m')}))

# Function to extract humidity
def extract_humidity(image):
    date = image.get('date')
    reduced = image.reduceRegions(
        collection=roswell_grid,
        reducer=ee.Reducer.mean(),
        scale=9000
    )
    return reduced.map(lambda f: f.set({'date': date, 'Humidity_mm': f.get('total_precipitable_water')}))

# Extract temperature and humidity
temperature_data = dailyTemperature.map(extract_temperature).flatten()
humidity_data = humidityCollection.map(extract_humidity).flatten()

# Join on 'date'
join_filter = ee.Filter.equals(leftField='date', rightField='date')
joined_data = ee.Join.inner().apply(temperature_data, humidity_data, join_filter)

# Export to Google Drive
task = ee.batch.Export.table.toDrive(
    collection=joined_data,
    description='Temp_Humidity_Only_Valid_Methane_Dates_Roswell',
    fileFormat='CSV'
)
task.start()

print("Export started! Only valid methane dates will be included!")
