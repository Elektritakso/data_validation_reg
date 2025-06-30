# Data Validation & Regulation Tool

A powerful Flask-based web application for validating CSV data against multiple regulatory frameworks with advanced error reporting and CSV export capabilities.

## 🚀 Features

- **Multi-Regulation Support**: Colombia, Peru, and Basic (IMS) validation frameworks
- **Advanced CSV Processing**: Pandas-powered with parallel processing for large datasets
- **Smart Error Detection**: Shows actual invalid values in error reports
- **CSV Export**: Download detailed error reports as CSV files
- **Encoding Support**: Handles international characters and multiple file encodings
- **Real-time Validation**: Fast validation with detailed error breakdowns

## 📁 Project Structure

```
├── app.py                    # Main Flask application
├── data_processor.py         # Optimized CSV processing with pandas
├── parallel_validator.py     # Multi-threaded validation engine
├── validator_registry.py     # Regulation-specific validator registry
├── validators_common.py      # Common validation functions
├── validators_colombia.py    # Colombia-specific validators
├── validators_peru.py        # Peru-specific validators
├── validators_ims.py         # Basic IMS validators
├── requirements.txt          # Python dependencies
├── static/
│   ├── css/styles.css       # Custom styling
│   └── js/script.js         # Frontend JavaScript
└── templates/
    └── index.html           # Main HTML template
```

## 🛠 Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python app.py
```

3. Open browser to `http://localhost:5000`

## 💡 Usage

1. **Select Regulation**: Choose from Colombia, Peru, or Basic validation
2. **Upload CSV**: Select your CSV file for validation
3. **Map Columns**: Map CSV columns to required fields (if needed)
4. **Validate**: Run validation and review results
5. **Download Errors**: Export detailed error report as CSV

## 🔧 Validation Rules

### Required Fields (All Regulations)
- `code`: Unique identifier (max 50 chars)
- `firstname`: Name (2-50 chars, no numbers)
- `lastname`: Name (2-50 chars, no numbers) 
- `email`: Valid email format

### Regulation-Specific Fields
- **Colombia**: Additional fields for citizenship, regional codes, ID cards
- **Peru**: Conditional PersonalID based on citizenship, document requirements
- **Basic**: Minimal validation for basic use cases

## 🚀 Performance Features

- **Parallel Processing**: Multi-core validation for large datasets
- **Memory Optimization**: Efficient handling of files >100MB
- **Smart Encoding**: Auto-detection and fallback for international files
- **Error Caching**: Temporary file storage to prevent session overflow

## 📊 Output Features

- **Error Summary**: Categorized error counts and types
- **Invalid Rows Display**: First 100 invalid rows with specific error details
- **CSV Export**: Complete error report with row numbers and invalid values
- **Duplicate Detection**: Email, username, and ID duplicate checking
