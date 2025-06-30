import re
from datetime import datetime
import logging
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for better performance
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-áéíóúñÁÉÍÓÚÑ$]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
CURRENCY_CODE_PATTERN = re.compile(r'^[A-Z]{3}$')
PHONE_PATTERN = re.compile(r'^\+?\d[\d\s\-\(\)]{8,20}$')
COUNTRY_CODE_PATTERN = re.compile(r'^[A-Z]{2}$')
LANGUAGE_CODE_PATTERN = re.compile(r'^[a-z]{2}$')
NAME_PATTERN = re.compile(r'^[a-zA-ZáéíóúñÁÉÍÓÚÑ\s\-\'\.]+$')
ZIP_CODE_PATTERN = re.compile(r'^[a-zA-Z0-9\s\-]{3,10}$')
REGION_PROVINCE_CODE_PATTERN = re.compile(r'^[A-Za-z0-9\-_]{1,10}$')
PERSONAL_ID_PATTERN = re.compile(r'^[A-Za-z0-9\-\s]+$')

def is_valid_email(email):
    """Check if the email has a valid format"""
    if email is None or email == '':
        return "Email is empty"
    
    email = str(email).strip()
    
    if '@' not in email:
        return "Email missing @ symbol"
    
    if not EMAIL_PATTERN.match(email):
        return "Email has invalid format"
    
    return None

def validate_currency_code(code):
    """Check if the currency code is a valid format"""
    if code is None or code == '':
        return "Cannot be empty"
    
    code = str(code).strip().upper()
    
    if not CURRENCY_CODE_PATTERN.match(code):
        return "Not a valid format (should be 3 uppercase letters)"
    
    return None

def is_invalid_birthdate(birthdate_str, age_limit=18):
    """Check if birthdate is invalid or represents someone underage"""
    if birthdate_str is None or birthdate_str == '':
        return "Birthdate is empty"
    
    birthdate_str = str(birthdate_str).strip()
    
    date_formats = ['%d/%m/%Y', '%m/%d/%Y'] # kas siin peaks formaate juurde lisama ?
    
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
    
    return "Invalid birthdate format"

def validate_phone_number(phone):
    """Check if the phone number is a valid format"""
    if phone is None or phone == '':
        return "Phone number is empty"
    
    phone = str(phone).strip()
    
    if not PHONE_PATTERN.match(phone):
        return "Phone number has invalid format"
    
    return None

def check_for_crlf(value):
    """Check for CRLF injection in the value"""
    if value is None:
        return None
    
    value_str = str(value)
    if '\r' in value_str or '\n' in value_str:
        return "Contains line breaks (potential CRLF injection)"
    
    return None

def validate_country_code(code):
    """Check if the country code is a valid format"""
    if code is None or code == '':
        return "Country code is empty"
    
    code = str(code).strip().upper()
    
    if not COUNTRY_CODE_PATTERN.match(code):
        return "Country code must be 2 uppercase letters"
    
    return None

def validate_language_code(code):
    """Check if the language code is a valid format"""
    if code is None or code == '':
        return "Language code is empty"
    
    code = str(code).strip().lower()
    
    if not LANGUAGE_CODE_PATTERN.match(code):
        return "Language code must be 2 lowercase letters"
    
    return None

def validate_name(name, max_length=50):
    """Check if the name is valid"""
    if name is None or name == '':
        return "Name is empty"
    
    name = str(name).strip()
    
    if len(name) > max_length:
        return f"Name too long (max {max_length} characters)"
    
    if not NAME_PATTERN.match(name):
        return "Name contains invalid characters"
    
    return None

def validate_signup_date(signup_date_str):
    """Check if signup date is valid and not in the future"""
    if signup_date_str is None or signup_date_str == '':
        return "Signup date is empty"
    
    signup_date_str = str(signup_date_str).strip()
    
    date_formats = ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d']
    
    for date_format in date_formats:
        try:
            signup_date = datetime.strptime(signup_date_str, date_format)
            today = datetime.today()
            
            if signup_date > today:
                return "Signup date is in the future"
            
            return None
            
        except ValueError:
            continue
    
    return "Invalid signup date format"

def validate_name_length(name, min_length=1, max_length=50):
    """Check if the name length is within acceptable bounds"""
    if name is None or name == '':
        return f"Name must be at least {min_length} character(s)"
    
    name = str(name).strip()
    
    if len(name) < min_length:
        return f"Name too short (min {min_length} characters)"
    
    if len(name) > max_length:
        return f"Name too long (max {max_length} characters)"
    
    return None

def validate_zip_code(zip_code):
    """Check if the zip code is valid"""
    if zip_code is None or zip_code == '':
        return "Zip code is empty"
    
    zip_code = str(zip_code).strip()
    
    if not ZIP_CODE_PATTERN.match(zip_code):
        return "Zip code has invalid format"
    
    return None

def validate_address_fields(row):
    """Validate that address fields are consistent"""
    errors = []
    
    address = row.get('address', '')
    city = row.get('city', '')
    
    if address and not city:
        errors.append("Address provided but city is missing")
    elif city and not address:
        errors.append("City provided but address is missing")
    
    return errors

def validate_language_country_consistency(row):
    """Validate that language and country codes are consistent"""
    errors = []
    
    language = row.get('languagecode', '')
    country = row.get('countrycode', '')
    
    if language and country:
        language = str(language).strip().lower()
        country = str(country).strip().upper()
        
        # Common language-country mappings
        language_country_map = {
            'es': ['ES', 'MX', 'AR', 'CO', 'PE', 'CL', 'VE'],
            'en': ['US', 'GB', 'CA', 'AU', 'NZ'],
            'fr': ['FR', 'CA', 'BE', 'CH'],
            'de': ['DE', 'AT', 'CH'],
            'it': ['IT', 'CH'],
            'pt': ['PT', 'BR']
        }
        
        if language in language_country_map:
            if country not in language_country_map[language]:
                errors.append(f"Language '{language}' not typically used in country '{country}'")
    
    return errors

def enhanced_email_validation(email):
    """Enhanced email validation with additional checks"""
    basic_error = is_valid_email(email)
    if basic_error:
        return basic_error
    
    email = str(email).strip().lower()
    
    # Check for disposable email domains
    disposable_domains = ['tempmail.org', '10minutemail.com', 'guerrillamail.com']
    domain = email.split('@')[1]
    
    if domain in disposable_domains:
        return "Disposable email addresses are not allowed"
    
    return None

def validate_citizenship(citizenship):
    """Check if the citizenship code is valid"""
    if citizenship is None or citizenship == '':
        return "Citizenship is empty"
    
    citizenship = str(citizenship).strip().upper()
    
    if not COUNTRY_CODE_PATTERN.match(citizenship):
        return "Citizenship must be 2 uppercase letters"
    
    return None

def validate_regioncode(regioncode):
    """Check if the region code is valid"""
    if regioncode is None or regioncode == '':
        return "Region code is empty"
    
    regioncode = str(regioncode).strip()
    
    if not REGION_PROVINCE_CODE_PATTERN.match(regioncode):
        return "Region code contains invalid characters or is too long"
    
    return None

def validate_provincecode(provincecode):
    """Check if the province code is valid"""
    if provincecode is None or provincecode == '':
        return "Province code is empty"
    
    provincecode = str(provincecode).strip()
    
    if not REGION_PROVINCE_CODE_PATTERN.match(provincecode):
        return "Province code contains invalid characters or is too long"
    
    return None

def validate_province(province):
    """Check if the province name is valid"""
    if province is None or province == '':
        return "Province is empty"
    
    province = str(province).strip()
    
    if len(province) < 2:
        return "Province name too short"
    
    if len(province) > 50:
        return "Province name too long"
    
    if not NAME_PATTERN.match(province):
        return "Province contains invalid characters"
    
    return None

def validate_personalid(personalid):
    """Default personal ID validation"""
    if personalid is None or personalid == '':
        return "Personal ID is empty"
    
    personalid = str(personalid).strip()
    
    if len(personalid) < 5:
        return "Personal ID too short"
    
    if len(personalid) > 20:
        return "Personal ID too long"
    
    if not PERSONAL_ID_PATTERN.match(personalid):
        return "Personal ID contains invalid characters"
    
    return None

def validate_idcardno(idcardno):
    """Check if the ID card number is valid"""
    if idcardno is None or idcardno == '':
        return "ID card number is empty"
    
    idcardno = str(idcardno).strip()
    
    if len(idcardno) < 5:
        return "ID card number too short"
    
    if len(idcardno) > 20:
        return "ID card number too long"
    
    if not PERSONAL_ID_PATTERN.match(idcardno):
        return "ID card number contains invalid characters"
    
    return None
