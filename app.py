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


# Import the new validator architecture
from validator_registry import validator_registry
from validators_common import *

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2048 * 1024 * 1024 
app.secret_key = os.urandom(24)  
logging.basicConfig(level=logging.DEBUG)

TEMP_FOLDER = os.path.join(tempfile.gettempdir(), 'csv_validator')
os.makedirs(TEMP_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv'}

REGULATIONS = {
    "CO": {
        "name": "Columbia",
        "required_fields": ["firstname", "lastname", 
                            "email", "birthdate", "address",
                            "city", "countrycode", "zip", "cellphone",
                            "gender", "username",
                            "birthdate", "citizenship", "regioncode",
                            "provincecode", "province", "personalid", 
                            "idcardno", 
                            "birthcity", "birthcountrycode"],
        "validations": {
            "birthdate": {"required": True, "min_age": 18},
            "email": {"required": True},
            "countrycode": {"required": True, "pattern": "^[A-Z]{2}$"},
            "gender": {"required": True, "values": ["M", "F"]},
            "citizenship": {"required": True, "pattern": "^[A-Z]{2}$"},
            "regioncode": {"required": True, "pattern": "^[0-9]{1,20}$"},
            "provincecode": {"required": True, "pattern": "^[0-9]{1,20}$"},
            "province": {"required": True, "max_length": 100},
            "personalid": {"required": True, "pattern": "^[A-Z0-9]{5,15}$", "max_length": 10, "min_length": 6},
            "idcardno": {"required": True, "pattern": "^[0-9]{6,12}$", "max_length": 10, "min_length": 6 },
            "birthcity": {"required": True, "max_length": 50},
            "birthcountrycode": {"required": True, "pattern": "^[A-Z]{2}$"},
            "cellphone": {"required": True, "pattern": "^[0-9+]{7,15}$"},
            "zip": {"required": True, "pattern": "^[0-9]{5,8}$"}
        }
    },

    "PE": {
        "name": "Peru",
        "required_fields": ["firstname", "lastname", 
                            "email", "birthdate", "address",
                            "city", "countrycode",
                            "username",
                            "citizenship",
                            "provincecode", "province", "personalid", 
                            "idcardno", "regioncode"],
        "validations": {
            "birthdate": {"required": True,"min_age": 18},
            "email": {"required": True},
            "countrycode": {"required": True, "pattern": "^[A-Z]{2}$"},
            "citizenship": {"required": True, "pattern": "^[A-Z]{2}$"},
            "regioncode": {"required": True, "pattern": "^[0-9]{1,20}$"},
            "provincecode": {"required": True, "pattern": "^[0-9]{1,20}$"},
            "province": {"required": True, "max_length": 100},
            "personalid": {"conditional": True, "depends_on": "citizenship", "pattern": "^[A-Z0-9]{5,15}$", "max_length": 10, "min_length": 6},
            "idcardno": {"pattern": "^[0-9]{6,12}$", "max_length": 10, "min_length": 6},
            "passportid": {"pattern": "^[A-Z0-9]{6,15}$", "max_length": 15, "min_length": 6},
            "DRIVERLICENSENO": {"pattern": "^[A-Z0-9]{8,20}$", "max_length": 20, "min_length": 8}
           
                            }
    },
    "IMS": {
        "name": "Basic",
        "required_fields": ["email", "firstname", "lastname"],
        "validations": {
            "email": {"required": True}
        }
    }
}

# Enable regulations feature
ENABLE_REGULATIONS = True

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

def check_duplicate_personalids(data):
    """
    Check for duplicate personal IDs in the data
    
    Args:
        data: List of dictionaries containing row data
        
    Returns:
        Dictionary mapping personal IDs to lists of row indices where they appear
    """
    personalid_indices = {}
    
    for idx, row in enumerate(data):
        if 'personalid' not in row or not row['personalid']:
            continue
            
        personalid = str(row['personalid']).strip()
        
        if personalid in personalid_indices:
            personalid_indices[personalid].append(idx)
        else:
            personalid_indices[personalid] = [idx]
    
    duplicate_personalids = {personalid: indices for personalid, indices in personalid_indices.items() if len(indices) > 1}
    app.logger.debug(f"Found {len(duplicate_personalids)} duplicate personal IDs")
    if duplicate_personalids:
        app.logger.debug(f"Sample duplicate personal IDs: {list(duplicate_personalids.keys())[:3]}")
    
    return duplicate_personalids

def check_duplicate_idcardnos(data):
    """
    Check for duplicate ID card numbers in the data
    
    Args:
        data: List of dictionaries containing row data
        
    Returns:
        Dictionary mapping ID card numbers to lists of row indices where they appear
    """
    idcardno_indices = {}
    
    for idx, row in enumerate(data):
        if 'idcardno' not in row or not row['idcardno']:
            continue
            
        idcardno = str(row['idcardno']).strip()
        
        if idcardno in idcardno_indices:
            idcardno_indices[idcardno].append(idx)
        else:
            idcardno_indices[idcardno] = [idx]
    
    duplicate_idcardnos = {idcardno: indices for idcardno, indices in idcardno_indices.items() if len(indices) > 1}
    app.logger.debug(f"Found {len(duplicate_idcardnos)} duplicate ID card numbers")
    if duplicate_idcardnos:
        app.logger.debug(f"Sample duplicate ID card numbers: {list(duplicate_idcardnos.keys())[:3]}")
    
    return duplicate_idcardnos



@app.route('/')
def index():
    """Render the form page"""
    return render_template('index.html')

@app.route('/regulations', methods=['GET'])
def get_regulations():
    """Get all available regulations"""
    if not ENABLE_REGULATIONS:
        return jsonify({'error': 'Regulations feature is disabled'}), 404
    
    return jsonify({
        'regulations': {code: reg["name"] for code, reg in REGULATIONS.items()}
    })

@app.route('/regulation-fields/<regulation_code>', methods=['GET'])
def get_regulation_fields(regulation_code):
    """Get required fields for a specific regulation"""
    if not ENABLE_REGULATIONS:
        return jsonify({'error': 'Regulations feature is disabled'}), 404
    
    regulation = REGULATIONS.get(regulation_code)
    if not regulation:
        return jsonify({'error': f'Regulation {regulation_code} not found'}), 404
    
    response = {
        'regulation': regulation_code,
        'name': regulation['name'],
        'required_fields': regulation['required_fields']
    }
    
    # Add is_custom flag if present
    if 'is_custom' in regulation:
        response['is_custom'] = regulation['is_custom']
    
    return jsonify(response)

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

        # Get the selected regulation
        selected_regulation = None
        if request.json and 'regulation' in request.json:
            regulation_key = request.json['regulation']
            if regulation_key in REGULATIONS:
                selected_regulation = REGULATIONS[regulation_key]
                app.logger.info(f"Using regulation: {regulation_key}")
        
        # If regulation is selected, use its required fields
        if selected_regulation:
            required_columns = selected_regulation.get('required_fields', [])
            session['required_columns'] = required_columns
            app.logger.info(f"Using regulation-specific required columns: {required_columns}")
        elif request.json and 'required_columns' in request.json:
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
        personalid_indices = {}
        duplicate_personalids = {}
        idcardno_indices = {}
        duplicate_idcardnos = {}
        
        validation_results = []
        row_idx = 0
        
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
                
                # Track emails
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    if email in email_indices:
                        if email not in duplicate_emails:
                            duplicate_emails[email] = [email_indices[email]]
                        duplicate_emails[email].append(current_idx)
                    else:
                        email_indices[email] = current_idx
                
                # Track usernames
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    if username in username_indices:
                        if username not in duplicate_usernames:
                            duplicate_usernames[username] = [username_indices[username]]
                        duplicate_usernames[username].append(current_idx)
                    else:
                        username_indices[username] = current_idx
                
                # Track personal IDs
                if 'personalid' in row and row['personalid']:
                    personalid = str(row['personalid']).strip()
                    if personalid in personalid_indices:
                        if personalid not in duplicate_personalids:
                            duplicate_personalids[personalid] = [personalid_indices[personalid]]
                        duplicate_personalids[personalid].append(current_idx)
                    else:
                        personalid_indices[personalid] = current_idx
                
                # Track ID card numbers
                if 'idcardno' in row and row['idcardno']:
                    idcardno = str(row['idcardno']).strip()
                    if idcardno in idcardno_indices:
                        if idcardno not in duplicate_idcardnos:
                            duplicate_idcardnos[idcardno] = [idcardno_indices[idcardno]]
                        duplicate_idcardnos[idcardno].append(current_idx)
                    else:
                        idcardno_indices[idcardno] = current_idx
            
            for i, row in enumerate(mapped_chunk):
                current_idx = row_idx + i
                row_errors = []
                is_valid = True
                code_value = row.get('code', 'N/A')
                
                # Check for duplicate emails
                if 'email' in row and row['email']:
                    email = str(row['email']).strip().lower()
                    if email in duplicate_emails:
                        if duplicate_emails[email][0] != current_idx:
                            error_msg = f"Duplicate email: {email} (also in row {duplicate_emails[email][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate email"] += 1
                            is_valid = False
                
                # Check for duplicate usernames
                if 'username' in row and row['username']:
                    username = str(row['username']).strip().lower()
                    if username in duplicate_usernames:
                        if duplicate_usernames[username][0] != current_idx:
                            error_msg = f"Duplicate username: {username} (also in row {duplicate_usernames[username][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate username"] += 1
                            is_valid = False
                
                # Check for duplicate personal IDs
                if 'personalid' in row and row['personalid']:
                    personalid = str(row['personalid']).strip()
                    if personalid in duplicate_personalids:
                        if duplicate_personalids[personalid][0] != current_idx:
                            error_msg = f"Duplicate personal ID: {personalid} (also in row {duplicate_personalids[personalid][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate personal ID"] += 1
                            is_valid = False
                
                # Check for duplicate ID card numbers
                if 'idcardno' in row and row['idcardno']:
                    idcardno = str(row['idcardno']).strip()
                    if idcardno in duplicate_idcardnos:
                        if duplicate_idcardnos[idcardno][0] != current_idx:
                            error_msg = f"Duplicate ID card number: {idcardno} (also in row {duplicate_idcardnos[idcardno][0] + 1})"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter["Duplicate ID card number"] += 1
                            is_valid = False
                
                for field in row.keys():
                    if field is None or not should_validate(field):
                        continue
                    
                    value = row[field]
                    # Special handling for Peru PersonalID - conditional requirement
                    if (field.lower() == 'personalid' and 
                        selected_regulation and selected_regulation.get('name') == 'Peru'):
                        # For Peru, PersonalID requirement depends on citizenship
                        citizenship = row.get('citizenship', '')
                        if str(citizenship).strip().upper() == 'ES' and not str(value).strip():
                            error_msg = f"{field} is required for Spanish residents"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                            continue
                        elif not str(value).strip():
                            # For non-Spanish residents, PersonalID is optional - skip validation
                            continue
                    elif not str(value).strip():
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
                        name_error = validate_name(value)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        length_error = validate_name_length(value, 1, 50)
                        if length_error:
                            row_errors.append(f"{code_value} {length_error}")
                            error_counter[length_error] += 1
                            is_valid = False

                    elif field_lower == 'regioncode':
                        regioncode_error = validate_regioncode(value)
                        if regioncode_error:
                            error_msg = f"regioncode: {regioncode_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'provincecode':
                        provincecode_error = validate_provincecode(value)
                        if provincecode_error:
                            error_msg = f"provincecode: {provincecode_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'province':
                        province_error = validate_province(value)
                        if province_error:
                            error_msg = f"province: {province_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'personalid':
                        # Use validator registry for regulation-specific validation
                        regulation_name = selected_regulation.get('name') if selected_regulation else None
                        personalid_validator = validator_registry.get_validator(regulation_name, 'personalid')
                        
                        if personalid_validator:
                            # Check if conditional validation is needed
                            conditional_info = validator_registry.has_conditional_validation(regulation_name, 'personalid')
                            if conditional_info and conditional_info.get('conditional'):
                                depends_on = conditional_info.get('depends_on')
                                dependency_value = row.get(depends_on, '') if depends_on else ''
                                personalid_error = personalid_validator(value, dependency_value)
                            else:
                                personalid_error = personalid_validator(value)
                        else:
                            # Use default personalid validation
                            personalid_error = validate_personalid(value)
                        
                        if personalid_error:
                            error_msg = f"personalid: {personalid_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'idcardno':
                        idcardno_error = validate_idcardno(value)
                        if idcardno_error:
                            error_msg = f"idcardno: {idcardno_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'citizenship':
                        citizenship_error = validate_citizenship(value)
                        if citizenship_error:
                            error_msg = f"citizenship: {citizenship_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False

                    elif field_lower == 'signupdate':
                        signup_date_error = validate_signup_date(value)
                        if signup_date_error:
                            error_msg = f"signupdate: {signup_date_error}"
                            row_errors.append(f"{code_value} {error_msg}")
                            error_counter[error_msg] += 1
                            is_valid = False
                    
                    elif field_lower == 'lastname':
                        name_error = validate_name(value)
                        if name_error:
                            row_errors.append(f"{code_value} {name_error}")
                            error_counter[name_error] += 1
                            is_valid = False
                        
                        length_error = validate_name_length(value, 1, 50)
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
                        # Use validator registry for regulation-specific zip validation
                        regulation_name = selected_regulation.get('name') if selected_regulation else None
                        zip_validator = validator_registry.get_validator(regulation_name, 'zip')
                        
                        if zip_validator:
                            zip_error = zip_validator(value)
                        else:
                            # Use default zip validation
                            zip_error = validate_zip_code(value)
                        
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
                
                # Regulation-specific document validation using registry
                regulation_name = selected_regulation.get('name') if selected_regulation else None
                if regulation_name:
                    document_validator = validator_registry.get_validator(regulation_name, 'documents')
                    if document_validator:
                        doc_error = document_validator(row)
                        if doc_error:
                            row_errors.append(f"{code_value} {doc_error}")
                            error_counter[doc_error] += 1
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
        duplicate_personalid_count = len(duplicate_personalids)
        duplicate_idcardno_count = len(duplicate_idcardnos)
        
        return jsonify({
            'validation_complete': True,
            'total_rows': len(validation_results),
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'error_counts': error_counts_sorted,
            'duplicate_email_count': duplicate_email_count,
            'duplicate_username_count': duplicate_username_count,
            'duplicate_personalid_count': duplicate_personalid_count,
            'duplicate_idcardno_count': duplicate_idcardno_count,
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
