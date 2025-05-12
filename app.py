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
from dateutil.relativedelta import relativedelta


app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2048 * 1024 * 1024 
app.secret_key = os.urandom(24)  
logging.basicConfig(level=logging.DEBUG)

TEMP_FOLDER = os.path.join(tempfile.gettempdir(), 'csv_validator')
os.makedirs(TEMP_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def detect_delimiter(sample):
    """Detect the delimiter in a CSV sample text"""
    possible_delimiters = [',', ';', '\t', '|']
    delimiter_counts = {delimiter: sample.count(delimiter) for delimiter in possible_delimiters}
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
    possible_enclosures = ['"', "'", '']
    
    try:
        if isinstance(file_content, bytes):
            try:
                content_str = file_content.decode('utf-8')
            except UnicodeDecodeError:
                content_str = file_content.decode('latin-1')
        else:
            content_str = file_content
            lines = content_str.split('\n')[:sample_lines+1]
        if not lines:
            return '"'  
            
        enclosure_counts = {enclosure: 0 for enclosure in possible_enclosures}
        
        header = lines[0]
        fields = header.split(delimiter)
        
        for enclosure in possible_enclosures:
            if enclosure:  
                enclosed_fields = 0
                for field in fields:
                    field = field.strip()
                    if field.startswith(enclosure) and field.endswith(enclosure):
                        enclosed_fields += 1
                        
                if enclosed_fields == len(fields):
                    enclosure_counts[enclosure] += 10  
                else:
                    enclosure_counts[enclosure] += enclosed_fields / len(fields)
            
        for line in lines[1:]:
            if not line.strip():  
                continue
                
            fields = line.split(delimiter)
            
            for enclosure in possible_enclosures:
                if enclosure:  
                    enclosed_fields = 0
                    for field in fields:
                        field = field.strip()
                        if len(field) >= 2 and field.startswith(enclosure) and field.endswith(enclosure):
                            enclosed_fields += 1
                            
                    enclosure_counts[enclosure] += enclosed_fields / len(fields)
                else:
                    non_enclosed = 0
                    for field in fields:
                        field = field.strip()
                        if not any(field.startswith(e) and field.endswith(e) for e in possible_enclosures if e):
                            non_enclosed += 1
                    
                    enclosure_counts[''] += non_enclosed / len(fields)
        
        app.logger.debug(f"Enclosure detection counts: {enclosure_counts}")
        
        max_enclosure = max(enclosure_counts.items(), key=lambda x: x[1])
        
        if max_enclosure[1] > 1:  
            return max_enclosure[0]
        else:
            return '"'  
            
    except Exception as e:
        app.logger.error(f"Error detecting enclosure: {str(e)}")
        return '"'  

def check_duplicate_emails(data):
    """
    Check for duplicate email addresses in the data
    
    Args:
        data: List of dictionaries containing row data
        
    Returns:
        Dictionary mapping emails to lists of row indices where they appear
    """
    email_indices = {}
    app.logger.debug(f"First few rows for duplicate check: {data[:2] if len(data) > 1 else data}")
    
    for idx, row in enumerate(data):
        if 'email' not in row or not row['email']:
            continue
            
        email = str(row['email']).strip().lower()  
        
        if email in email_indices:
            email_indices[email].append(idx)
        else:
            email_indices[email] = [idx]
    
    duplicate_emails = {email: indices for email, indices in email_indices.items() if len(indices) > 1}
    
    app.logger.debug(f"Found {len(duplicate_emails)} duplicate emails")
    if duplicate_emails:
        app.logger.debug(f"Sample duplicate emails: {list(duplicate_emails.keys())[:3]}")
    
    return duplicate_emails

def validate_signup_date(date_str):
    """
    Validate that a signup date is not in the future
    
    Args:
        date_str: The date string to validate
        
    Returns:
        Error message if invalid, None if valid
    """
    if date_str is None or date_str == '':
        return None  
    
    date_str = str(date_str).strip()
    
    date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%d-%m-%Y']
    
    for date_format in date_formats:
        try:
            signup_date = datetime.strptime(date_str, date_format)
            today = datetime.today()
            
            if signup_date > today:
                return "Signup date cannot be in the future"
            
            return None
            
        except ValueError:
            continue
    
    return f"Invalid signup date format: '{date_str}'"


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
        if 'username' not in row or not row['username']:
            continue
            
        username = str(row['username']).strip().lower()  
        
        if username in username_indices:
            username_indices[username].append(idx)
        else:
            username_indices[username] = [idx]
    
    duplicate_usernames = {username: indices for username, indices in username_indices.items() if len(indices) > 1}
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
        return "Cannot be empty"
    
    code = str(code).strip().upper()
    
    if not re.match(r'^[A-Z]{3}$', code):
        return "Not a valid format (should be 3 uppercase letters)"
    
    return None


def is_invalid_birthdate(birthdate_str, age_limit=18):
    """Check if birthdate is invalid or represents someone underage"""
    if birthdate_str is None or birthdate_str == '':
        return "Birthdate is empty"
    
    birthdate_str = str(birthdate_str).strip()
    
    date_formats = ['%d/%m/%Y'] # kas siin peaks formaate juurde lisama ?
    
    for date_format in date_formats:
        try:
            birthdate = datetime.strptime(birthdate_str, date_format)
            today = datetime.today()
            
            if birthdate > today:
                return "Birthdate is in the future"
            
            age_delta = relativedelta(today, birthdate)
            age = age_delta.years
            
            if age < age_limit:
                return f"Account holder is underage (age: {age})"
            
            return None
            
        except ValueError:
            continue
    
    return f"Invalid birthdate format: '{birthdate_str}'"

def validate_phone_number(phone):
    """Check if the phone number contains only digits and optional + sign"""
    if phone is None or phone == '':
        return None
    
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
        return "Cannot be empty" 
    
    code = str(code).strip().upper()

    if code == "00":
        return None  # "00" on meil default vist?
    
    if not re.match(r'^[A-Z]{2}$', code):
        return "Not a valid format"
    
    return None

def validate_language_code(code):
    """Check if the language code is a valid format"""
    if code is None or code == '':
        return "Cannot be empty"  
    
    code = str(code).strip().lower()
    
    if re.match(r'^[a-zA-Z]{2}$', code):
        return None
    
    if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}$', code):
        return None

    return "Not a valid format"
def validate_placeholder(value):
    """Check if a value is just a placeholder (-)"""
    if value == '-':
        return "Contains only placeholder '-'"
    return None

def validate_name(name, field):
    """Validate person names for common issues"""
    if name is None or name == '':
        return f"{field} is empty"
        
    name = str(name).strip()
    
    placeholders = ['-', 'n/a', 'na', 'none', 'unknown', 'test', 'user']
    if name.lower() in placeholders:
        return f"{field} contains placeholder value"
        
    if re.search(r'\d', name):
        return f"{field} contains numbers"
        
    if len(re.findall(r'[^\w\s]', name)) > 2:
        return f"{field} contains too many special characters"
        
    if len(name) < 2:
        return f"{field} is too short"
        
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
        app.logger.debug(f"request.form keys: {list(request.form.keys())}")
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Please upload CSV files'}), 400
        
        chunk_size = 10 * 1024 
        file_content_bytes = file.read(chunk_size)
        
        encoding = detect_encoding(file_content_bytes)
        
        try:
            file_content_sample = file_content_bytes.decode(encoding)
        except UnicodeDecodeError:
            file_content_sample = file_content_bytes.decode('utf-8', errors='replace')
        
        sample = '\n'.join(file_content_sample.split('\n')[:5])
        delimiter = detect_delimiter(sample)
        
        enclosure = detect_enclosure(file_content_bytes, delimiter)
        
        file.seek(0)
        file_id = str(uuid.uuid4())
        temp_file_path = os.path.join(TEMP_FOLDER, f"{file_id}.csv")
        
        with open(temp_file_path, 'wb') as temp_file:
            chunk = file.read(8192)
            while chunk:
                temp_file.write(chunk)
                chunk = file.read(8192)
        
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            header_line = f.readline().strip()
            if enclosure and enclosure != '':
                columns = next(csv.reader([header_line], delimiter=delimiter, quotechar=enclosure))
            else:
                columns = header_line.split(delimiter)
        
        if not columns:
            return jsonify({'error': 'No valid headers found in the CSV file'}), 400
        
        app.logger.debug(f"CSV columns found: {columns}")
        
        required_columns = []
        if 'required_columns' in request.form:
            required_columns_input = request.form['required_columns']
            app.logger.debug(f"Raw required_columns from form: {required_columns_input}")
            
            try:
                parsed_columns = json.loads(required_columns_input)
                if isinstance(parsed_columns, list):
                    required_columns = [col.strip() for col in parsed_columns if col.strip()]
                else:
                    required_columns = [str(parsed_columns).strip()]
            except json.JSONDecodeError:
                app.logger.warning(f"Failed to parse as JSON: {required_columns_input}")
                required_columns = [col.strip() for col in required_columns_input.split(',') if col.strip()]
            
            app.logger.info(f"Required columns parsed: {required_columns}")
            
            session['required_columns'] = required_columns

        column_mappings = {}
        if 'column_mappings' in request.form:
            mappings_input = request.form['column_mappings']
            app.logger.debug(f"Raw column_mappings from form: {mappings_input}")
            
            try:
                column_mappings = json.loads(mappings_input)
                app.logger.info(f"Column mappings parsed: {column_mappings}")
            except json.JSONDecodeError:
                app.logger.warning(f"Failed to parse column mappings: {mappings_input}")
        
        session['column_mappings'] = column_mappings
        
        cleaned_columns = []
        for col in columns:
            cleaned_col = col.strip()
            for enclosure_char in ['"', "'"]:
                if cleaned_col.startswith(enclosure_char) and cleaned_col.endswith(enclosure_char):
                    cleaned_col = cleaned_col[1:-1]  
            cleaned_columns.append(cleaned_col)
        
        app.logger.debug(f"Original CSV columns: {columns}")
        app.logger.debug(f"Cleaned CSV columns: {cleaned_columns}")
        app.logger.debug(f"Required columns after parsing: {required_columns}")
        
        available_columns_set = set(col.lower() for col in cleaned_columns)
        
        for csv_col, mapped_col in column_mappings.items():
            if csv_col.lower() in available_columns_set:
                available_columns_set.add(mapped_col.lower())
        
        app.logger.debug(f"Available columns after mapping: {available_columns_set}")
        
        missing_columns = []
        for req_col in required_columns:
            req_col_lower = req_col.lower()
            if req_col_lower not in available_columns_set:
                missing_columns.append(req_col) 
        
        app.logger.debug(f"Missing columns check after mapping: {missing_columns}")
        if missing_columns:
            return jsonify({
                'error': 'Required columns missing in file',
                'missing_columns': missing_columns,
                'available_columns': columns  
            }), 400
        
        row_count = 0
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            next(f)
            for _ in f:
                row_count += 1
        
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
        
        preview_data = []
        with open(temp_file_path, 'r', encoding=encoding, errors='replace') as f:
            if enclosure and enclosure != '':
                reader = csv.DictReader(f, delimiter=delimiter, quotechar=enclosure)
            else:
                reader = csv.DictReader(f, delimiter=delimiter)
            
            for i, row in enumerate(reader):
                if i >= 10: 
                    break
                preview_data.append(row)
        
        return jsonify({
            'message': 'File uploaded successfully',
            'file_id': file_id,
            'rows': row_count,
            'data': preview_data,
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
        app.logger.debug(f"Session keys at validation start: {list(session.keys())}")
        app.logger.debug(f"Required columns from session: {session.get('required_columns', [])}")
        
        if 'current_file' not in session:
            return jsonify({'error': 'No file has been uploaded. Please upload a file first.'}), 400
        
        file_info = session['current_file']
        file_path = file_info['path']
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'File no longer exists. Please upload again.'}), 400
        
        error_counter = Counter()
        
        chunk_size = 5000  
        
        def read_csv_in_chunks(file_path, delimiter, chunk_size):
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                chunk = []
                for i, row in enumerate(reader):
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                if chunk:  
                    yield chunk
        
        if request.json and 'required_columns' in request.json:
            required_columns = request.json['required_columns']
            session['required_columns'] = required_columns
            app.logger.info(f"Using required columns from request: {required_columns}")
        else:
            required_columns = session.get('required_columns', [])
            app.logger.info(f"Using required columns from session: {required_columns}")
        
        if not required_columns:
            required_columns = ['code', 'firstname', 'lastname', 'email',
                            'birthdate', 'address', 'city', 'phone', 'cellphone',
                            'countrycode', 'signuplanguagecode', 'currencycode','username', 'zip', 'signupdate', 'password']
        
        required_columns_lower = [col.lower() for col in required_columns]
        
        app.logger.info(f"Validating ONLY these columns: {required_columns}")
        
        column_mappings = {}
        if request.json and 'column_mappings' in request.json:
            try:
                column_mappings = request.json['column_mappings']
                app.logger.info(f"Using column mappings from request: {column_mappings}")
                session['column_mappings'] = column_mappings
            except Exception as e:
                app.logger.error(f"Error processing column mappings from request: {str(e)}")
                column_mappings = session.get('column_mappings', {})
                app.logger.info(f"Falling back to session column mappings: {column_mappings}")
        else:
            column_mappings = session.get('column_mappings', {})
            app.logger.info(f"Using column mappings from session: {column_mappings}")
        
        def should_validate(column_name):
            if column_name is None:
                return False
            return column_name.lower() in required_columns_lower
        
        def map_column_name(original_name):
            if original_name is None:
                return original_name
            for csv_header, expected_field in column_mappings.items():
                if csv_header is None:
                    continue
                if original_name.lower() == csv_header.lower():
                    return expected_field
            return original_name
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=file_info['delimiter'])
            all_columns = [col.strip() for col in reader.fieldnames]
        
        email_indices = {}
        duplicate_emails = {}
        username_indices = {}
        duplicate_usernames = {}
        
        validation_results = []
        row_idx = 0
        
        def validate_name_length(name, field_name, max_length=50):
            """Validate that a name field doesn't exceed the maximum length"""
            if name is None or name == '':
                return None  
            
            name = str(name).strip()
            
            if len(name) > max_length:
                return f"{field_name} exceeds maximum length of {max_length} characters"
            
            return None
            
        
        def validate_language_code(code):
            """Check if the language code is a valid format"""
            if code is None or code == '':
                return "Cannot be empty"  
            
            code = str(code).strip()
            
            if re.match(r'^[a-zA-Z]{2}$', code):
                return None
            
            if re.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}$', code):
                return None
            
            return "Not a valid format"
        
        def validate_zip_code(zip_code, country_code=None):
            """
            Validate zip/postal code format based on country code if provided
            
            Args:
                zip_code: The zip/postal code to validate
                country_code: Optional country code to apply country-specific validation
                
            Returns:
                Error message if invalid, None if valid
            """
            if zip_code is None or zip_code == '':
                return "Zip/postal code is empty"
            
            zip_code = str(zip_code).strip()
            
            if country_code:
                country_code = str(country_code).strip().upper()
                
                if country_code == 'US':
                    if not re.match(r'^\d{5}(?:-\d{4})?$', zip_code):
                        return "Invalid US ZIP code format (should be 5 digits or ZIP+4)"
                
                elif country_code == 'CA':
                    # Remove spaces for validation
                    zip_no_spaces = zip_code.replace(' ', '')
                    if not re.match(r'^[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d$', zip_no_spaces):
                        return "Invalid Canadian postal code format"
                
                elif country_code == 'GB':
                    if not re.match(r'^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$', zip_code.upper()):
                        return "Invalid UK postal code format"
                
                elif country_code == 'AU':
                    if not re.match(r'^\d{4}$', zip_code):
                        return "Invalid Australian postal code format (should be 4 digits)"
                
                elif country_code == 'DE':
                    if not re.match(r'^\d{5}$', zip_code):
                        return "Invalid German postal code format (should be 5 digits)"
                
                elif country_code == 'FR':
                    if not re.match(r'^\d{5}$', zip_code):
                        return "Invalid French postal code format (should be 5 digits)"
                
                elif country_code == 'IT':
                    if not re.match(r'^\d{5}$', zip_code):
                        return "Invalid Italian postal code format (should be 5 digits)"
                
                elif country_code == 'ES':
                    if not re.match(r'^\d{5}$', zip_code):
                        return "Invalid Spanish postal code format (should be 5 digits)"

            if not re.match(r'^[A-Za-z0-9\s-]+$', zip_code):
                return "Zip/postal code contains invalid characters"
            
            return None


        
        def validate_address_fields(row):
            """Validate address, city"""
            errors = []
            
            address = row.get('address', '')
            city = row.get('city', '')
            country_code = row.get('countrycode', '')

            if address:
                if len(address) < 2:
                    errors.append("Address is too short (minimum 2 characters)")
                elif len(address) > 640:
                    errors.append("Address is too long (maximum 640 characters)")
            
            if city:
                if len(city) < 2:
                    errors.append("City name is too short (minimum 2 characters)")
                elif len(city) > 50:
                    errors.append("City name is too long (maximum 50 characters)")
            
            # Check for common test addresses
            #test_addresses = ['123 main', 'test address', '123 test', 'main street']
            #if address and any(test_addr in address.lower() for test_addr in test_addresses):
                #errors.append("Address appears to be a test/placeholder")
            
            if country_code:
                country_code = country_code.strip().upper()
            
            return errors if errors else None
        
        def validate_language_country_consistency(row):
            """Check if language code is consistent with country code"""
            return None
            
            language_code = row.get('signuplanguagecode', '')
            country_code = row.get('countrycode', '')
            
            if not language_code or not country_code:
                return None
            
            base_language = language_code.strip().lower().split('-')[0]
            country_code = country_code.strip().upper()
            
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
            
            disposable_domains = [
                'mailinator.com', 'yopmail.com', 'tempmail.com', 'guerrillamail.com',
                'temp-mail.org', 'fakeinbox.com', 'sharklasers.com', 'trashmail.com',
                'mailnesia.com', '10minutemail.com', 'tempinbox.com', 'dispostable.com'
            ]
            
            domain = email.split('@')[-1]
            if domain in disposable_domains:
                return f"Email uses disposable domain: {domain}"
            
            test_patterns = [
                r'^test', r'test@', r'example@', r'user@', r'sample@',
                r'@example\.', r'@test\.', r'@localhost'
            ]
            
            for pattern in test_patterns:
                if re.search(pattern, email):
                    return "Email appears to be a test address"
        
        for chunk in read_csv_in_chunks(file_path, file_info['delimiter'], chunk_size):
            mapped_chunk = []
            for original_row in chunk:
                mapped_row = {}
                for key, value in original_row.items():
                    mapped_key = map_column_name(key)
                    mapped_row[mapped_key] = value
                mapped_chunk.append(mapped_row)
            
            for i, row in enumerate(mapped_chunk):
                current_idx = row_idx + i
                
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    
                    if email in email_indices:
                        if email not in duplicate_emails:
                            duplicate_emails[email] = [email_indices[email]]
                        duplicate_emails[email].append(current_idx)
                    else:
                        email_indices[email] = current_idx
                
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    
                    if username in username_indices:
                        if username not in duplicate_usernames:
                            duplicate_usernames[username] = [username_indices[username]]
                        duplicate_usernames[username].append(current_idx)
                    else:
                        username_indices[username] = current_idx
            
            for i, row in enumerate(mapped_chunk):
                current_idx = row_idx + i
                row_errors = []
                is_valid = True
                code_value = row.get('code', 'N/A')
                
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    if email in duplicate_emails:
                        if duplicate_emails[email][0] != current_idx:
                            error_msg = f"Duplicate email: {email} (also in row {duplicate_emails[email][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate email"] += 1
                            is_valid = False
                
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    if username in duplicate_usernames:
                        if duplicate_usernames[username][0] != current_idx:
                            error_msg = f"Duplicate username: {username} (also in row {duplicate_usernames[username][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate username"] += 1
                            is_valid = False
                
                for field in row.keys():
                    if field is None or not should_validate(field):
                        continue
                    
                    value = row[field]
                    if not str(value).strip():
                        error_msg = f"{field} is required but missing"
                        row_errors.append(f"{code_value} {error_msg}")
                        error_counter[error_msg] += 1
                        is_valid = False
                        continue
                    
                    field_lower = field.lower() if field is not None else ""
                    
                    if field_lower == 'email':
                        email_error = enhanced_email_validation(value)
                        if email_error:
                            row_errors.append(f"{code_value} {email_error}")
                            error_counter[email_error] += 1
                            is_valid = False
                    
                    elif field_lower == 'birthdate':
                        birthdate_error = is_invalid_birthdate(value)
                        if birthdate_error:
                            error_msg = "Invalid birthdate"
                            detailed_error = f"Birthdate: {birthdate_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[detailed_error] += 1
                            is_valid = False
                    
                    elif field_lower in ['phone', 'cellphone']:
                        phone_error = validate_phone_number(value)
                        if phone_error:
                            error_msg = f"{field}: {phone_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    elif field_lower == 'firstname':
                        name_error = validate_name(value, field)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        length_error = validate_name_length(value, 'firstname', 50)
                        if length_error:
                            row_errors.append(f"{code_value} {length_error}")
                            error_counter[length_error] += 1
                            is_valid = False

                    elif field_lower == 'signupdate':
                        signup_date_error = validate_signup_date(value)
                        if signup_date_error:
                            error_msg = f"signupdate: {signup_date_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    
                    elif field_lower == 'lastname':
                        name_error = validate_name(value, field)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        length_error = validate_name_length(value, 'lastname', 50)
                        if length_error:
                            row_errors.append(f"{code_value} {length_error}")
                            error_counter[length_error] += 1
                            is_valid = False
                    
                    elif field_lower == 'address':
                        crlf_error = check_for_crlf(value)
                        if crlf_error:
                            error_msg = f"address: {crlf_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    elif field_lower == 'countrycode':
                        country_error = validate_country_code(value)
                        if country_error:
                            error_msg = f"countrycode: {country_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower in ['zipcode', 'postalcode', 'zip', 'postcode']:
                        country_code = None
                        for key, value in row.items():
                            if key and key.lower() == 'countrycode':
                                country_code = value
                                break
                        
                        zip_error = validate_zip_code(value, country_code)
                        if zip_error:
                            error_msg = f"{field}: {zip_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    elif field_lower == 'signuplanguagecode':
                        lang_error = validate_language_code(value)
                        if lang_error:
                            error_msg = f"signuplanguagecode: {lang_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    elif field_lower == 'currencycode':
                        currency_error = validate_currency_code(value)
                        if currency_error:
                            error_msg = f"currencycode: {currency_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
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
            
            row_idx += len(mapped_chunk)
        
        valid_rows = sum(1 for result in validation_results if result['valid'])
        invalid_rows = len(validation_results) - valid_rows
        
        error_counts_sorted = dict(sorted(error_counter.items(), key=lambda x: x[1], reverse=True))
        
        duplicate_email_count = len(duplicate_emails)
        duplicate_username_count = len(duplicate_usernames)
        
        return jsonify({
            'validation_complete': True,
            'total_rows': len(validation_results),
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'error_counts': error_counts_sorted,
            'duplicate_email_count': duplicate_email_count,
            'duplicate_username_count': duplicate_username_count,
            'results': validation_results[:100],
            'all_columns': all_columns,
            'required_columns': required_columns,
            'column_mappings': column_mappings  
        })
    except Exception as e:
        app.logger.error(f"Validation error: {str(e)}", exc_info=True)
        return jsonify({'error': f'Error during validation: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)
