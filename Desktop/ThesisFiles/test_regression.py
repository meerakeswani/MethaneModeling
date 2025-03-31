import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

# Step 1: Load data
file_path_methane = '/Users/meerakeswani/Desktop/ThesisFiles/CH4_Roswell_2018_2021_clean2.csv'
data_methane = pd.read_csv(file_path_methane)

file_path_humidity_temp = '/Users/meerakeswani/Desktop/ThesisFiles/Temp_Humidity_Only_Valid_Methane_Dates_Roswell.csv'
data_humidity_temp = pd.read_csv(file_path_humidity_temp)
# Step 2: Extract features and target
X = data_humidity_temp[['Temperature', 'Humidity']]
y = data_methane['mean']

# Step 3: Manual 2/3 - 1/3 split
split_index = int(len(data) * (2/3))
X_train = X.iloc[:split_index]
y_train = y.iloc[:split_index]
X_test = X.iloc[split_index:]
y_test = y.iloc[split_index:]

print(f"Training samples: {len(X_train)}, Validation samples: {len(X_test)}")

# Step 4: Create and train the Linear Regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Step 5: Predict on validation set
y_pred = model.predict(X_test)

# Step 6: Evaluate performance
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print("Mean Squared Error on validation set:", mse)
print("R2 Score on validation set:", r2)
print("Coefficients (Temperature, Humidity):", model.coef_)
print("Intercept:", model.intercept_)

# Step 7: Plot actual vs predicted
plt.scatter(y_test, y_pred)
plt.xlabel('Actual Methane Values')
plt.ylabel('Predicted Methane Values')
plt.title('Actual vs Predicted Methane (Validation Set)')
plt.grid(True)
plt.show()
