from flask import Flask, render_template, request, jsonify, session
import csv
import io
import re
from datetime import datetime
import traceback
import chardet
import os
import uuid
import tempfile
import logging
from collections import Counter
import json

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2048 * 1024 * 1024 
app.secret_key = os.urandom(24)  # For session management

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create a temporary folder for storing uploaded files
TEMP_FOLDER = os.path.join(tempfile.gettempdir(), 'csv_validator')
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'txt'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def detect_delimiter(sample):
    """Detect the delimiter in a CSV sample text"""
    possible_delimiters = [',', ';', '\t', '|']
    delimiter_counts = {delimiter: sample.count(delimiter) for delimiter in possible_delimiters}
    
    # Return the delimiter with the highest count
    return max(delimiter_counts.items(), key=lambda x: x[1])[0]

def detect_encoding(file_content):
    """Detect the encoding of file content"""
    result = chardet.detect(file_content)
    return result['encoding'] or 'utf-8'

def detect_enclosure(file_content, delimiter=',', sample_lines=5):
    """
    Detect the enclosure character used in CSV content.
    
    Args:
        file_content: The CSV file content as bytes
        delimiter: The CSV delimiter character
        sample_lines: Number of lines to sample for detection
    
    Returns:
        str: Detected enclosure character or empty string if none detected
    """
    # Common enclosure characters to check
    possible_enclosures = ['"', "'", '']
    
    try:
        # Convert bytes to string if needed
        if isinstance(file_content, bytes):
            # Try to decode with utf-8 first, then fallback to latin-1
            try:
                content_str = file_content.decode('utf-8')
            except UnicodeDecodeError:
                content_str = file_content.decode('latin-1')
        else:
            content_str = file_content
            
        # Get first few lines for analysis
        lines = content_str.split('\n')[:sample_lines+1]
        if not lines:
            return '"'  # Default to double quotes if no lines
            
        # Count enclosure patterns in the sample
        enclosure_counts = {enclosure: 0 for enclosure in possible_enclosures}
        
        # First analyze header row
        header = lines[0]
        fields = header.split(delimiter)
        
        for enclosure in possible_enclosures:
            if enclosure:  # Skip empty enclosure in header analysis
                enclosed_fields = 0
                for field in fields:
                    field = field.strip()
                    if field.startswith(enclosure) and field.endswith(enclosure):
                        enclosed_fields += 1
                        
                # If all fields are enclosed, this is likely the enclosure
                if enclosed_fields == len(fields):
                    enclosure_counts[enclosure] += 10  # Give strong weight to header pattern
                else:
                    enclosure_counts[enclosure] += enclosed_fields / len(fields)
            
        # Then analyze data rows
        for line in lines[1:]:
            if not line.strip():  # Skip empty lines
                continue
                
            fields = line.split(delimiter)
            
            for enclosure in possible_enclosures:
                if enclosure:  # Skip empty enclosure check for performance
                    enclosed_fields = 0
                    for field in fields:
                        field = field.strip()
                        if len(field) >= 2 and field.startswith(enclosure) and field.endswith(enclosure):
                            enclosed_fields += 1
                            
                    # Add weight based on percentage of enclosed fields
                    enclosure_counts[enclosure] += enclosed_fields / len(fields)
                else:
                    # For empty enclosure, check if other enclosures are NOT present
                    non_enclosed = 0
                    for field in fields:
                        field = field.strip()
                        if not any(field.startswith(e) and field.endswith(e) for e in possible_enclosures if e):
                            non_enclosed += 1
                    
                    enclosure_counts[''] += non_enclosed / len(fields)
        
        # Log the results
        app.logger.debug(f"Enclosure detection counts: {enclosure_counts}")
        
        # Return the enclosure with the highest count, or empty string if none are significant
        max_enclosure = max(enclosure_counts.items(), key=lambda x: x[1])
        
        # If the count is significant, return that enclosure
        if max_enclosure[1] > 1:  # More than one line shows this pattern
            return max_enclosure[0]
        else:
            return '"'  # Default to double quotes if no clear pattern
            
    except Exception as e:
        app.logger.error(f"Error detecting enclosure: {str(e)}")
        return '"'  # Default to double quotes on error

def check_duplicate_emails(data):
    """
    Check for duplicate email addresses in the data
    
    Args:
        data: List of dictionaries containing row data
        
    Returns:
        Dictionary mapping emails to lists of row indices where they appear
    """
    email_indices = {}
    
    # Debug: Print the first few rows to check structure
    app.logger.debug(f"First few rows for duplicate check: {data[:2] if len(data) > 1 else data}")
    
    for idx, row in enumerate(data):
        # Skip if no email field or empty email
        if 'email' not in row or not row['email']:
            continue
            
        email = str(row['email']).strip().lower()  # Normalize email
        
        if email in email_indices:
            email_indices[email].append(idx)
        else:
            email_indices[email] = [idx]
    
    # Filter to only keep emails that appear more than once
    duplicate_emails = {email: indices for email, indices in email_indices.items() if len(indices) > 1}
    
    # Debug logging
    app.logger.debug(f"Found {len(duplicate_emails)} duplicate emails")
    if duplicate_emails:
        app.logger.debug(f"Sample duplicate emails: {list(duplicate_emails.keys())[:3]}")
    
    return duplicate_emails

def check_duplicate_usernames(data):
    """
    Check for duplicate usernames in the data
    
    Args:
        data: List of dictionaries containing row data
        
    Returns:
        Dictionary mapping usernames to lists of row indices where they appear
    """
    username_indices = {}
    
    for idx, row in enumerate(data):
        # Skip if no username field or empty username
        if 'username' not in row or not row['username']:
            continue
            
        username = str(row['username']).strip().lower()  # Normalize username
        
        if username in username_indices:
            username_indices[username].append(idx)
        else:
            username_indices[username] = [idx]
    
    # Filter to only keep usernames that appear more than once
    duplicate_usernames = {username: indices for username, indices in username_indices.items() if len(indices) > 1}
    
    # Debug logging
    app.logger.debug(f"Found {len(duplicate_usernames)} duplicate usernames")
    if duplicate_usernames:
        app.logger.debug(f"Sample duplicate usernames: {list(duplicate_usernames.keys())[:3]}")
    
    return duplicate_usernames


def is_valid_email(email):
    """Check if the email has a valid format"""
    if email is None or email == '':
        return "Email is empty"
    
    email = str(email).strip()
    
    if '@' not in email:
        return "Email missing @ symbol"
    
    if not re.match(r'^[a-zA-Z0-9._%+-áéíóúñÁÉÍÓÚÑ$]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return "Email has invalid format"
    
    return None

def validate_currency_code(code):
    """Check if the currency code is a valid format"""
    if code is None or code == '':
        return "Cannot be NULL or empty"
    
    code = str(code).strip().upper()
    
    if not re.match(r'^[A-Z]{3}$', code):
        return "Not a valid format (should be 3 uppercase letters)"
    
    return None


def is_invalid_birthdate(birthdate_str, age_limit=18):
    """Check if birthdate is invalid or represents someone underage"""
    if birthdate_str is None or birthdate_str == '':
        return "Birthdate is empty"
    
    # Clean the input
    birthdate_str = str(birthdate_str).strip()
    
    # Try multiple date formats
    date_formats = ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d-%m-%Y']
    
    for date_format in date_formats:
        try:
            birthdate = datetime.strptime(birthdate_str, date_format)
            today = datetime.today()
            
            # Check if date is in the future
            if birthdate > today:
                return "Birthdate is in the future"
            
            # Calculate age
            age = today.year - birthdate.year
            if (today.month, today.day) < (birthdate.month, birthdate.day):
                age -= 1
            
            # Check if underage
            if age < age_limit:
                return f"Account holder is underage (age: {age})"
            
            # If we got here, the date is valid and age is sufficient
            return None
            
        except ValueError:
            continue
    
    # If all formats fail
    return f"Invalid birthdate format: '{birthdate_str}'"

def validate_phone_number(phone):
    """Check if the phone number contains only digits and optional + sign"""
    if phone is None or phone == '':
        return None  # Empty phone is allowed
    
    phone = str(phone).strip()
    
    if ' ' in phone:
        return "Phone contains spaces"
    
    if not re.match(r'^[\d+]*$', phone):
        return "Phone contains invalid characters"
    
    return None

def check_for_crlf(text):
    """Check if text contains CRLF characters"""
    if text is None or text == '':
        return None
    
    text = str(text)
    if re.search(r'\r\n', text):
        return "Contains CRLF characters"
    
    return None

def validate_country_code(code):
    """Check if the country code is a valid format"""
    if code is None or code == '':
        return "Cannot be NULL or empty"  # Empty country code is allowed
    
    code = str(code).strip().upper()
    
    if not re.match(r'^[A-Z]{2}$', code):
        return "Not a valid format"
    
    return None

def validate_language_code(code):
    """Check if the language code is a valid format"""
    if code is None or code == '':
        return "Cannot be NULL or empty"  # Empty language code is allowed
    
    code = str(code).strip().lower()
    
 # Check for simple 2-letter language code (e.g., 'en', 'fr')
    if re.match(r'^[a-zA-Z]{2}$', code):
        return None
    
    # Check for language-country format (e.g., 'en-US', 'fr-CA', 'EN-GB')
    if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}$', code):
        return None
    
    # Check for extended language tags (e.g., 'en-GB-oed')
    if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-[a-zA-Z0-9]+$', code):
        return None

    return "Not a valid format"
def validate_placeholder(value, field_name):
    """Check if a value is just a placeholder (-)"""
    if value == '-':
        return "Contains only placeholder '-'"
    return None

def validate_name(name, field):
    """Validate person names for common issues"""
    if name is None or name == '':
        return f"{field} is empty"
        
    name = str(name).strip()
    
    # Check for placeholder values
    placeholders = ['-', 'n/a', 'na', 'none', 'unknown', 'test', 'user']
    if name.lower() in placeholders:
        return f"{field} contains placeholder value"
        
    # Check for numeric characters
    if re.search(r'\d', name):
        return f"{field} contains numbers"
        
    # Check for excessive punctuation
    if len(re.findall(r'[^\w\s]', name)) > 2:
        return f"{field} contains too many special characters"
        
    # Check for very short names
    if len(name) < 2:
        return f"{field} is too short"
        
    # Check for very long names
    if len(name) > 50:
        return f"{field} is too long"
        
    return None

@app.route('/')
def index():
    """Render the form page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        # Debug logging for request form contents
        app.logger.debug(f"request.form keys: {list(request.form.keys())}")
        
        # Check if file part exists
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400
        
        file = request.files['file']
        
        # Check if user submitted an empty form
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Check file type
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Please upload CSV files'}), 400
        
        # Read the first chunk to detect encoding, delimiter, etc.
        chunk_size = 10 * 1024  # 10KB should be enough for detection
        file_content_bytes = file.read(chunk_size)
        
        # Detect encoding
        encoding = detect_encoding(file_content_bytes)
        
        # Decode the content using detected encoding
        try:
            file_content_sample = file_content_bytes.decode(encoding)
        except UnicodeDecodeError:
            file_content_sample = file_content_bytes.decode('utf-8', errors='replace')
        
        # Detect delimiter from the first few lines
        sample = '\n'.join(file_content_sample.split('\n')[:5])
        delimiter = detect_delimiter(sample)
        
        # Detect enclosure character
        enclosure = detect_enclosure(file_content_bytes, delimiter)
        
        # Reset file pointer to beginning
        file.seek(0)
        
        # Create a temporary file to store the uploaded content
        file_id = str(uuid.uuid4())
        temp_file_path = os.path.join(TEMP_FOLDER, f"{file_id}.csv")
        
        # Process the file in chunks and write directly to the temp file
        with open(temp_file_path, 'wb') as temp_file:
            chunk = file.read(8192)  # Read 8KB at a time
            while chunk:
                temp_file.write(chunk)
                chunk = file.read(8192)
        
        # Now read the header to get columns
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            # Read just the header line
            header_line = f.readline().strip()
            # Parse the header using the detected delimiter
            if enclosure and enclosure != '':
                columns = next(csv.reader([header_line], delimiter=delimiter, quotechar=enclosure))
            else:
                columns = header_line.split(delimiter)
        
        if not columns:
            return jsonify({'error': 'No valid headers found in the CSV file'}), 400
        
        # Debug log the columns found in the file
        app.logger.debug(f"CSV columns found: {columns}")
        
        # Process required columns - improved parsing for JSON array strings
        required_columns = []
        if 'required_columns' in request.form:
            required_columns_input = request.form['required_columns']
            app.logger.debug(f"Raw required_columns from form: {required_columns_input}")
            
            try:
                # Try to parse as JSON
                parsed_columns = json.loads(required_columns_input)
                if isinstance(parsed_columns, list):
                    required_columns = [col.strip() for col in parsed_columns if col.strip()]
                else:
                    # If it's not a list, use the single value
                    required_columns = [str(parsed_columns).strip()]
            except json.JSONDecodeError:
                # If JSON parsing fails, fall back to comma splitting
                app.logger.warning(f"Failed to parse as JSON: {required_columns_input}")
                required_columns = [col.strip() for col in required_columns_input.split(',') if col.strip()]
            
            app.logger.info(f"Required columns parsed: {required_columns}")
            
            # Store the required columns in the session for later use
            session['required_columns'] = required_columns

        # Process column mappings
        column_mappings = {}
        if 'column_mappings' in request.form:
            mappings_input = request.form['column_mappings']
            app.logger.debug(f"Raw column_mappings from form: {mappings_input}")
            
            try:
                column_mappings = json.loads(mappings_input)
                app.logger.info(f"Column mappings parsed: {column_mappings}")
            except json.JSONDecodeError:
                app.logger.warning(f"Failed to parse column mappings: {mappings_input}")
        
        # Store mappings in session
        session['column_mappings'] = column_mappings
        
        # Clean column names for comparison
        cleaned_columns = []
        for col in columns:
            # Strip common enclosure characters (", ', etc.)
            cleaned_col = col.strip()
            for enclosure_char in ['"', "'"]:
                if cleaned_col.startswith(enclosure_char) and cleaned_col.endswith(enclosure_char):
                    cleaned_col = cleaned_col[1:-1]  # Remove first and last character
            cleaned_columns.append(cleaned_col)
        
        app.logger.debug(f"Original CSV columns: {columns}")
        app.logger.debug(f"Cleaned CSV columns: {cleaned_columns}")
        app.logger.debug(f"Required columns after parsing: {required_columns}")
        
        # Create a set of available columns (original + mapped)
        available_columns_set = set(col.lower() for col in cleaned_columns)
        
        # Add mapped columns to the available set
        for csv_col, mapped_col in column_mappings.items():
            if csv_col.lower() in available_columns_set:
                available_columns_set.add(mapped_col.lower())
        
        app.logger.debug(f"Available columns after mapping: {available_columns_set}")
        
        # Check for missing columns using cleaned names (case-insensitive)
        missing_columns = []
        for req_col in required_columns:
            req_col_lower = req_col.lower()
            if req_col_lower not in available_columns_set:
                missing_columns.append(req_col)  # Report original name
        
        app.logger.debug(f"Missing columns check after mapping: {missing_columns}")
        if missing_columns:
            return jsonify({
                'error': 'Required columns missing in file',
                'missing_columns': missing_columns,
                'available_columns': columns  # Keep original names in response
            }), 400
        
        # Count the number of rows in the file
        row_count = 0
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            # Skip header
            next(f)
            # Count lines
            for _ in f:
                row_count += 1
        
        # Store file info in session
        session['current_file'] = {
            'id': file_id,
            'path': temp_file_path,
            'rows': row_count,
            'columns': columns,
            'delimiter': delimiter,
            'encoding': encoding,
            'enclosure': enclosure,
            'column_mappings': column_mappings
        }
        
        # Read a small sample for preview (first 10 rows)
        preview_data = []
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            if enclosure and enclosure != '':
                reader = csv.DictReader(f, delimiter=delimiter, quotechar=enclosure)
            else:
                reader = csv.DictReader(f, delimiter=delimiter)
            
            for i, row in enumerate(reader):
                if i >= 10:  # Only read first 10 rows for preview
                    break
                preview_data.append(row)
        
        # Return successful response with preview data
        return jsonify({
            'message': 'File uploaded successfully',
            'file_id': file_id,
            'rows': row_count,
            'data': preview_data,  # Return only first 10 rows for preview
            'columns': columns,
            'detected_delimiter': delimiter,
            'detected_encoding': encoding,
            'detected_enclosure': enclosure,
            'column_mappings': column_mappings
        })
        
    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        app.logger.error(traceback.format_exc())
        return jsonify({'error': f'Error processing file: {str(e)}'}), 500




@app.route('/validate', methods=['POST'])
def validate():
    """Validate data from the stored file - only checking specified columns"""
    try:
        # Debug logging for session contents
        app.logger.debug(f"Session keys at validation start: {list(session.keys())}")
        app.logger.debug(f"Required columns from session: {session.get('required_columns', [])}")
        
        # Check if we have a file in the session
        if 'current_file' not in session:
            return jsonify({'error': 'No file has been uploaded. Please upload a file first.'}), 400
        
        file_info = session['current_file']
        file_path = file_info['path']
        
        # Ensure the file exists
        if not os.path.exists(file_path):
            return jsonify({'error': 'File no longer exists. Please upload again.'}), 400
        
        # Initialize error counter
        error_counter = Counter()
        
        # Process in chunks
        chunk_size = 5000  # Increase from current 1000
        
        # Use a generator to read the file in chunks
        def read_csv_in_chunks(file_path, delimiter, chunk_size):
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                chunk = []
                for i, row in enumerate(reader):
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                if chunk:  # Don't forget the last chunk
                    yield chunk
        
        # Get the required columns from the request JSON (new columns may have been added)
        if request.json and 'required_columns' in request.json:
            required_columns = request.json['required_columns']
            # Update the session with the new required columns
            session['required_columns'] = required_columns
            app.logger.info(f"Using required columns from request: {required_columns}")
        else:
            # If not provided in request, get from session
            required_columns = session.get('required_columns', [])
            app.logger.info(f"Using required columns from session: {required_columns}")
        
        # If no required columns were specified by the user, fallback to defaults
        if not required_columns:
            required_columns = ['code', 'firstname', 'lastname', 'email',
                            'birthdate', 'address', 'city', 'phone', 'cellphone',
                            'countrycode', 'signuplanguagecode']
        
        # Convert to lowercase for case-insensitive comparison
        required_columns_lower = [col.lower() for col in required_columns]
        
        app.logger.info(f"Validating ONLY these columns: {required_columns}")
        
        # Get column mappings from request or session
        column_mappings = {}
        if request.json and 'column_mappings' in request.json:
            try:
                column_mappings = request.json['column_mappings']
                app.logger.info(f"Using column mappings from request: {column_mappings}")
                session['column_mappings'] = column_mappings
            except Exception as e:
                app.logger.error(f"Error processing column mappings from request: {str(e)}")
                # Fall back to session mappings
                column_mappings = session.get('column_mappings', {})
                app.logger.info(f"Falling back to session column mappings: {column_mappings}")
        else:
            column_mappings = session.get('column_mappings', {})
            app.logger.info(f"Using column mappings from session: {column_mappings}")
        
        # Helper function to check if a column should be validated
        def should_validate(column_name):
            if column_name is None:
                return False
            return column_name.lower() in required_columns_lower
        
        # Fix for map_column_name function
        def map_column_name(original_name):
            if original_name is None:
                return original_name
            # Check if this column should be mapped
            for csv_header, expected_field in column_mappings.items():
                if csv_header is None:
                    continue
                if original_name.lower() == csv_header.lower():
                    return expected_field
            return original_name
        
        # Get all column names for reporting
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=file_info['delimiter'])
            all_columns = [col.strip() for col in reader.fieldnames]
        
        # Track emails and usernames for duplicate detection
        email_indices = {}
        duplicate_emails = {}
        username_indices = {}
        duplicate_usernames = {}
        
        # Process each chunk
        validation_results = []
        row_idx = 0
        
        # New validation functions
        def validate_name_length(name, field_name, max_length=50):
            """Validate that a name field doesn't exceed the maximum length"""
            if name is None or name == '':
                return None  # Empty check is handled elsewhere
            
            name = str(name).strip()
            
            if len(name) > max_length:
                return f"{field_name} exceeds maximum length of {max_length} characters"
            
            return None
        
        def validate_language_code(code):
            """Check if the language code is a valid format"""
            if code is None or code == '':
                return "Cannot be NULL or empty"  # Empty language code is allowed
            
            code = str(code).strip()
            
            # Check for simple 2-letter language code (e.g., 'en', 'fr')
            if re.match(r'^[a-zA-Z]{2}$', code):
                return None
            
            # Check for language-country format (e.g., 'en-US', 'fr-CA', 'EN-GB')
            if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}$', code):
                return None
            
            # Check for extended language tags (e.g., 'en-GB-oed')
            if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-[a-zA-Z0-9]+$', code):
                return None
            
            return "Not a valid format"
        
        def validate_phone_numbers(row):
            """Validate phone and cellphone fields for consistency"""
            errors = []
            
            phone = row.get('phone', '')
            cellphone = row.get('cellphone', '')
            country_code = row.get('countrycode', '')
            
            # Check if both phone fields are empty
            if (not phone or phone.strip() == '') and (not cellphone or cellphone.strip() == ''):
                errors.append("Both phone and cellphone are empty")
                return errors
            
            # Check for duplicate phone numbers
            if phone and cellphone and phone.strip() == cellphone.strip():
                errors.append("Phone and cellphone are identical")
            
            # Country-specific phone validations
            if country_code:
                country_code = country_code.strip().upper()
                
                # US phone validation
                if country_code == 'US':
                    for phone_field, phone_value in [('phone', phone), ('cellphone', cellphone)]:
                        if phone_value:
                            # Remove all non-digits
                            digits_only = re.sub(r'\D', '', phone_value)
                            if len(digits_only) not in (10, 11):
                                errors.append(f"{phone_field} does not match US format")
                
                # UK phone validation
                elif country_code == 'GB':
                    for phone_field, phone_value in [('phone', phone), ('cellphone', cellphone)]:
                        if phone_value:
                            # UK numbers typically start with 0 or +44
                            if not (phone_value.startswith('0') or '+44' in phone_value or '44' == phone_value[:2]):
                                errors.append(f"{phone_field} does not match UK format")
            
            return errors if errors else None
        
        def validate_address_fields(row):
            """Validate address, city, and country for consistency"""
            errors = []
            
            address = row.get('address', '')
            city = row.get('city', '')
            country_code = row.get('countrycode', '')
            
            # Check if address is empty but city is not
            if (not address or address.strip() == '') and city and city.strip() != '':
                errors.append("Address is empty but city is provided")
            
            # Check if city is empty but address is not
            if (not city or city.strip() == '') and address and address.strip() != '':
                errors.append("City is empty but address is provided")
            
            # Check for PO Box addresses
            if address and re.search(r'p\.?o\.?\s*box', address.lower()):
                errors.append("Address is a PO Box")
            
            # Check for common test addresses
            test_addresses = ['123 main', 'test address', '123 test', 'main street']
            if address and any(test_addr in address.lower() for test_addr in test_addresses):
                errors.append("Address appears to be a test/placeholder")
            
            # Country-specific address validations
            if country_code:
                country_code = country_code.strip().upper()
                
                # US address should typically have a number at the beginning
                if country_code == 'US' and address and not re.match(r'^\d+', address):
                    errors.append("US address should typically start with a number")
            
            return errors if errors else None
        
        def validate_language_country_consistency(row):
            """Check if language code is consistent with country code"""
            errors = []
            
            language_code = row.get('signuplanguagecode', '')
            country_code = row.get('countrycode', '')
            
            if not language_code or not country_code:
                return None
            
            # Extract base language code if in format like "en-GB"
            base_language = language_code.strip().lower().split('-')[0]
            country_code = country_code.strip().upper()
            
            # Common country-language mappings
            country_language_map = {
                'US': ['en'],
                'GB': ['en'],
                'CA': ['en', 'fr'],
                'FR': ['fr'],
                'DE': ['de'],
                'ES': ['es'],
                'IT': ['it'],
                'PT': ['pt'],
                'BR': ['pt'],
                'MX': ['es'],
                'JP': ['ja'],
                'CN': ['zh'],
                'RU': ['ru']
            }
            
            if country_code in country_language_map:
                expected_languages = country_language_map[country_code]
                if base_language not in expected_languages:
                    errors.append(f"Language code '{language_code}' unusual for country '{country_code}'")
            
            return errors if errors else None
        
        def enhanced_email_validation(email):
            """Enhanced validation for email addresses"""
            basic_error = is_valid_email(email)
            if basic_error:
                return basic_error
            
            email = str(email).strip().lower()
            
            # Check for disposable email domains
            disposable_domains = [
                'mailinator.com', 'yopmail.com', 'tempmail.com', 'guerrillamail.com',
                'temp-mail.org', 'fakeinbox.com', 'sharklasers.com', 'trashmail.com',
                'mailnesia.com', '10minutemail.com', 'tempinbox.com', 'dispostable.com'
            ]
            
            domain = email.split('@')[-1]
            if domain in disposable_domains:
                return f"Email uses disposable domain: {domain}"
            
            # Check for common test email patterns
            test_patterns = [
                r'^test', r'test@', r'example@', r'user@', r'sample@',
                r'@example\.', r'@test\.', r'@localhost'
            ]
            
            for pattern in test_patterns:
                if re.search(pattern, email):
                    return "Email appears to be a test address"
            
            # Check for numeric-only local part
            local_part = email.split('@')[0]
            if local_part.isdigit():
                return "Email username contains only numbers"
            
            return None
        
        def validate_player_code(code):
            """Validate player code format"""
            if code is None or code == '':
                return "Player code is empty"
            
            code = str(code).strip()
            
            # Check for minimum length (adjust based on your requirements)
            if len(code) < 4:
                return "Player code is too short"
            
            # Check if code follows expected pattern (adjust regex as needed)
            if not re.match(r'^[A-Za-z0-9_-]+$', code):
                return "Player code contains invalid characters"
            
            return None
        
        for chunk in read_csv_in_chunks(file_path, file_info['delimiter'], chunk_size):
            # Apply column mappings to chunk
            mapped_chunk = []
            for original_row in chunk:
                mapped_row = {}
                for key, value in original_row.items():
                    mapped_key = map_column_name(key)
                    mapped_row[mapped_key] = value
                mapped_chunk.append(mapped_row)
            
            # Check for duplicate emails and usernames in this chunk
            for i, row in enumerate(mapped_chunk):
                current_idx = row_idx + i
                
                # Check for duplicate emails
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    
                    if email in email_indices:
                        if email not in duplicate_emails:
                            duplicate_emails[email] = [email_indices[email]]
                        duplicate_emails[email].append(current_idx)
                    else:
                        email_indices[email] = current_idx
                
                # Check for duplicate usernames
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    
                    if username in username_indices:
                        if username not in duplicate_usernames:
                            duplicate_usernames[username] = [username_indices[username]]
                        duplicate_usernames[username].append(current_idx)
                    else:
                        username_indices[username] = current_idx
            
            # Validate each row in the chunk
            for i, row in enumerate(mapped_chunk):
                current_idx = row_idx + i
                row_errors = []
                is_valid = True
                code_value = row.get('code', 'N/A')
                
                # Check for duplicate email
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    if email in duplicate_emails:
                        # Only mark as duplicate if this isn't the first occurrence
                        if duplicate_emails[email][0] != current_idx:
                            error_msg = f"Duplicate email: {email} (also in row {duplicate_emails[email][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate email"] += 1
                            is_valid = False
                
                # Check for duplicate username
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    if username in duplicate_usernames:
                        # Only mark as duplicate if this isn't the first occurrence
                        if duplicate_usernames[username][0] != current_idx:
                            error_msg = f"Duplicate username: {username} (also in row {duplicate_usernames[username][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate username"] += 1
                            is_valid = False
                
                # Field-specific validations
                for field in row.keys():
                    # Skip fields not in required columns
                    if field is None or not should_validate(field):
                        continue
                    
                    # Required field check
                    value = row[field]
                    if not str(value).strip():
                        error_msg = f"{field} is required but missing"
                        row_errors.append(f"{code_value} {error_msg}")
                        error_counter[error_msg] += 1
                        is_valid = False
                        continue
                    
                    # Field-specific validations
                    field_lower = field.lower() if field is not None else ""
                    
                    # Player code validation
                    if field_lower == 'code':
                        code_error = validate_player_code(value)
                        if code_error:
                            error_msg = f"code: {code_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    # Enhanced email validation
                    elif field_lower == 'email':
                        email_error = enhanced_email_validation(value)
                        if email_error:
                            row_errors.append(f"{code_value} {email_error}")
                            error_counter[email_error] += 1
                            is_valid = False
                    
                    # Birthdate validation
                    elif field_lower == 'birthdate':
                        birthdate_error = is_invalid_birthdate(value)
                        if birthdate_error:
                            error_msg = "INVALID birthdate"
                            detailed_error = f"Birthdate: {birthdate_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[detailed_error] += 1
                            is_valid = False
                    
                    # Phone number validation
                    elif field_lower in ['phone', 'cellphone']:
                        phone_error = validate_phone_number(value)
                        if phone_error:
                            error_msg = f"{field}: {phone_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    # First name validation including length check
                    elif field_lower == 'firstname':
                        name_error = validate_name(value, field)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        # Additional length check
                        length_error = validate_name_length(value, 'firstname', 50)
                        if length_error:
                            row_errors.append(f"{code_value} {length_error}")
                            error_counter[length_error] += 1
                            is_valid = False
                    
                    # Last name validation including length check
                    elif field_lower == 'lastname':
                        name_error = validate_name(value, field)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        # Additional length check
                        length_error = validate_name_length(value, 'lastname', 50)
                        if length_error:
                            row_errors.append(f"{code_value} {length_error}")
                            error_counter[length_error] += 1
                            is_valid = False
                    
                    # Address CRLF check
                    elif field_lower == 'address':
                        crlf_error = check_for_crlf(value)
                        if crlf_error:
                            error_msg = f"address: {crlf_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    # Country code validation
                    elif field_lower == 'countrycode':
                        country_error = validate_country_code(value)
                        if country_error:
                            error_msg = f"countrycode: {country_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    # Language code validation (updated to support formats like EN-GB)
                    elif field_lower == 'signuplanguagecode':
                        lang_error = validate_language_code(value)
                        if lang_error:
                            error_msg = f"signuplanguagecode: {lang_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    # Currency code validation
                    elif field_lower == 'currencycode':
                        currency_error = validate_currency_code(value)
                        if currency_error:
                            error_msg = f"currencycode: {currency_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                
                # Add cross-field validations
                phone_errors = validate_phone_numbers(row)
                if phone_errors:
                    for error in phone_errors:
                        row_errors.append(f"{code_value} {error}")
                        error_counter[error] += 1
                        is_valid = False
                
                address_errors = validate_address_fields(row)
                if address_errors:
                    for error in address_errors:
                        row_errors.append(f"{code_value} {error}")
                        error_counter[error] += 1
                        is_valid = False
                
                language_country_errors = validate_language_country_consistency(row)
                if language_country_errors:
                    for error in language_country_errors:
                        row_errors.append(f"{code_value} {error}")
                        error_counter[error] += 1
                        is_valid = False
                
                validation_results.append({
                    'row': current_idx + 1,
                    'code': code_value,
                    'valid': is_valid,
                    'errors': row_errors
                })
            
            # Update row index for next chunk
            row_idx += len(mapped_chunk)
        
        # Calculate summary statistics
        valid_rows = sum(1 for result in validation_results if result['valid'])
        invalid_rows = len(validation_results) - valid_rows
        
        # Convert error counter to sorted dictionary
        error_counts_sorted = dict(sorted(error_counter.items(), key=lambda x: x[1], reverse=True))
        
        # Add duplicate counts to the response
        duplicate_email_count = len(duplicate_emails)
        duplicate_username_count = len(duplicate_usernames)
        
        # Explicitly include the required columns and column mappings in the response
        return jsonify({
            'validation_complete': True,
            'total_rows': len(validation_results),
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'error_counts': error_counts_sorted,
            'duplicate_email_count': duplicate_email_count,
            'duplicate_username_count': duplicate_username_count,
            'results': validation_results[:10],
            'all_columns': all_columns,
            'required_columns': required_columns,
            'column_mappings': column_mappings  # Include the column mappings in the response
        })
    except Exception as e:
        app.logger.error(f"Validation error: {str(e)}", exc_info=True)
        return jsonify({'error': f'Error during validation: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)
