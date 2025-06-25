import re
from datetime import datetime
import logging
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

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

def validate_citizenship(value):
    """Validate citizenship field"""
    if value is None or value == '':
        return "Citizenship required"
    
    value = str(value).strip().upper()

    if not re.match(r'^[A-Z]{2}$', value):
        return "Citizenship must be 2 letter countrycode"
    
    return None

def validate_regioncode(value):
    """Validate regioncode field (numeric, max 20 digits)"""
    if value is None or value == '':
        return "Regioncode is required"
    
    value = str(value).strip()
    
    if not value.isdigit():
        return "Regioncode must contain only numbers"
    
    if len(value) > 20:
        return "Regioncode must be maximum 20 digits"
    
    return None

def validate_provincecode(value):
    """Validate provincecode number max 20"""
    if value is None or value == '':
        return "Provincecode is required"
    
    value = str(value).strip()

    if not value.isdigit():
        return "Provincecode must be only numbers"
    
    if len(value) > 20:
        return "Max 20 digits"
    
    return None


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

def validate_name_length(name, field_name, max_length=50):
    """Validate that a name field doesn't exceed the maximum length"""
    if name is None or name == '':
        return None  
    
    name = str(name).strip()
    
    if len(name) > max_length:
        return f"{field_name} exceeds maximum length of {max_length} characters"
    
    return None

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
    
    if country_code:
        country_code = country_code.strip().upper()
    
    return errors if errors else None

def validate_language_country_consistency(row):
    """Check if language code is consistent with country code"""
    errors = []
    
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
    
    return None
