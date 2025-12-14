import pandas as pd

# ✅ Correct file location
input_file = "/Users/balodapreetam/Downloads/household_power_consumption.txt"

# ✅ Output cleaned file location
output_file = "/Users/balodapreetam/Documents/7 set project/household_power_consumption_cleaned.csv"

print("📥 Loading dataset...")

# 1️⃣ Read TXT file (semicolon separated & replace '?' with NaN)
df = pd.read_csv(
    input_file,
    sep=";",
    na_values="?",
    low_memory=False
)

print("✅ Dataset loaded")
print("Shape before cleaning:", df.shape)

# 2️⃣ Remove rows with missing values
print("🧹 Removing missing values...")
df = df.dropna()

print("✅ Missing rows removed")
print("Shape after cleaning:", df.shape)

# 3️⃣ Convert Date + Time to DateTime
print("🕒 Creating DateTime column...")
df["DateTime"] = pd.to_datetime(
    df["Date"] + " " + df["Time"],
    format="%d/%m/%Y %H:%M:%S"
)

# Remove old columns
df = df.drop(columns=["Date", "Time"])

# 4️⃣ Move DateTime to first column
cols = ["DateTime"] + [c for c in df.columns if c != "DateTime"]
df = df[cols]

# 5️⃣ Save cleaned file
df.to_csv(output_file, index=False)

print("✅ Cleaning completed successfully!")
print("📁 Cleaned file saved at:")
print(output_file)

print("\n👀 Sample Data:")
print(df.head())
