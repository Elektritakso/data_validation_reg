import re
from validators_common import validate_personalid

def validate_peru_personalid(personalid, citizenship):
    """
    Validate Peru PersonalID based on citizenship
    
    Args:
        personalid: The personal ID value
        citizenship: The citizenship code (e.g., 'ES' for Spain)
        
    Returns:
        Error message if invalid, None if valid
    """
    if citizenship is None or citizenship == '':
        return "Citizenship is required for PersonalID validation"
    
    citizenship = str(citizenship).strip().upper()
    
    # For Spanish residents (citizenship = ES)
    if citizenship == 'ES':
        if personalid is None or personalid == '':
            return "PersonalID is mandatory for Spanish residents (DNI or NIE required)"
        
        personalid = str(personalid).strip().upper()
        
        # Check for DNI format (8 digits + 1 letter)
        if re.match(r'^\d{8}[A-Z]$', personalid):
            return None  # Valid DNI format
        
        # Check for NIE format (1 letter + 7 digits + 1 letter)
        if re.match(r'^[XYZ]\d{7}[A-Z]$', personalid):
            return None  # Valid NIE format
        
        return "Invalid Spanish ID format. Must be DNI (8 digits + letter) or NIE (X/Y/Z + 7 digits + letter)"
    
    # For non-residents (citizenship != ES)
    else:
        if personalid is None or personalid == '':
            return None  # PersonalID is optional for non-residents
        
        personalid = str(personalid).strip()
        
        # For non-residents, accept various document types
        # Must be at least 5 characters and max 20 characters
        if len(personalid) < 5:
            return "PersonalID is too short (minimum 5 characters)"
        
        if len(personalid) > 20:
            return "PersonalID is too long (maximum 20 characters)"
        
        # Accept alphanumeric characters and common separators
        if not re.match(r'^[A-Za-z0-9\-\s]+$', personalid):
            return "PersonalID contains invalid characters"
        
        return None  # Valid for non-residents

def validate_peru_documents(row):
    """
    Validate that Peru non-residents have at least one required document
    
    Args:
        row: Dictionary containing the row data
        
    Returns:
        Error message if invalid, None if valid
    """
    citizenship = row.get('citizenship', '')
    if citizenship is None or citizenship == '':
        return None  # Skip if citizenship is not provided
    
    citizenship = str(citizenship).strip().upper()
    
    # For Spanish residents, no additional document requirement (PersonalID handles it)
    if citizenship == 'ES':
        return None
    
    # For non-residents, check if at least one document is provided
    documents = {
        'idcardno': row.get('idcardno', ''),
        'passportid': row.get('passportid', ''),
        'DRIVERLICENSENO': row.get('DRIVERLICENSENO', ''),
        'personalid': row.get('personalid', '')  # Can still be used for non-residents
    }
    
    # Check if at least one document has a value
    provided_documents = []
    for doc_name, doc_value in documents.items():
        if doc_value and str(doc_value).strip():
            provided_documents.append(doc_name)
    
    if not provided_documents:
        return "Non-residents must provide at least one document: ID card, passport, driver's license, or personal ID"
    
    return None  # At least one document is provided

def validate_peru_zip(zip_code):
    """
    Validate Peru-specific zip code format
    
    Args:
        zip_code: The zip code value
        
    Returns:
        Error message if invalid, None if valid
    """
    if zip_code is None or zip_code == '':
        return "Zip code is empty"
    
    zip_code = str(zip_code).strip()
    
    # Peru postal codes are 5 digits (LIMA format: LIMAxx where xx are digits)
    # or 6 digits for other regions
    if re.match(r'^LIMA\d{2}$', zip_code.upper()):
        return None  # Valid Lima format
    
    if re.match(r'^\d{5}$', zip_code):
        return None  # Valid 5-digit format
    
    if re.match(r'^\d{6}$', zip_code):
        return None  # Valid 6-digit format
    
    return "Peru zip code must be 5 digits, 6 digits, or LIMA format (LIMAxx)"
