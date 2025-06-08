"""
Regulations module for country-specific validation rules.
"""
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

REGULATIONS = {
    "EU": {
        "name": "European Union Regulations",
        "required_fields": ["firstname", "lastname", "email", "birthdate", "address", "city", "countrycode", "zip"],
        "validations": {
            "birthdate": {"min_age": 16},
            "email": {"required": True},
            "countrycode": {"pattern": "^[A-Z]{2}$"}
        }
    },
    "US": {
        "name": "United States Regulations",
        "required_fields": ["firstname", "lastname", "email", "address", "city", "zip", "state"],
        "validations": {
            "birthdate": {"min_age": 13},
            "zip": {"pattern": "^\\d{5}(?:-\\d{4})?$"},
            "state": {"required": True}
        }
    },
    "UK": {
        "name": "United Kingdom Regulations",
        "required_fields": ["firstname", "lastname", "email", "address", "city", "postcode"],
        "validations": {
            "birthdate": {"min_age": 13},
            "postcode": {"pattern": "^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$"}
        }
    }
    # Add more country regulations as needed
}

def get_regulation(code):
    """Get regulation by country code."""
    return REGULATIONS.get(code)

def get_all_regulations():
    """Get all available regulations."""
    return {code: reg["name"] for code, reg in REGULATIONS.items()}

def validate_field(field_name, value, regulation_code):
    """Validate a field according to regulation rules."""
    regulation = get_regulation(regulation_code)
    if not regulation:
        return None
        
    validations = regulation.get('validations', {})
    if field_name not in validations:
        return None
        
    rules = validations[field_name]
    
    # Check if required
    if rules.get('required', False) and (value is None or str(value).strip() == ''):
        return f"{field_name} is required by {regulation['name']}"
    
    # Check pattern if specified
    if 'pattern' in rules and value:
        pattern = rules['pattern']
        if not re.match(pattern, str(value).strip()):
            return f"{field_name} does not match required format for {regulation['name']}"
    
    # Check min_age for birthdate
    if field_name == 'birthdate' and 'min_age' in rules and value:
        # Try to parse the date
        date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y']
        birthdate = None
        
        for fmt in date_formats:
            try:
                birthdate = datetime.strptime(str(value).strip(), fmt)
                break
            except ValueError:
                continue
        
        if birthdate:
            today = datetime.today()
            age = relativedelta(today, birthdate).years
            if age < rules['min_age']:
                return f"Age must be at least {rules['min_age']} years (current: {age})"
        else:
            return f"Invalid birthdate format: {value}"
    
    return None
