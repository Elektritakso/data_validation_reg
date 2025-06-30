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
    
    if citizenship == 'ES':
        if personalid is None or personalid == '':
            return "PersonalID is mandatory for Spanish residents (DNI or NIE required)"
        
        personalid = str(personalid).strip().upper()
        
        if re.match(r'^\d{8}[A-Z]$', personalid):
            return None  
        
        if re.match(r'^[XYZ]\d{7}[A-Z]$', personalid):
            return None  
        
        return "Invalid Spanish ID format. Must be DNI (8 digits + letter) or NIE (X/Y/Z + 7 digits + letter)"
    
    else:
        if personalid is None or personalid == '':
            return None  
        
        personalid = str(personalid).strip()
        

        if len(personalid) < 5:
            return "PersonalID is too short (minimum 5 characters)"
        
        if len(personalid) > 20:
            return "PersonalID is too long (maximum 20 characters)"
        
        if not re.match(r'^[A-Za-z0-9\-\s]+$', personalid):
            return "PersonalID contains invalid characters"
        
        return None  

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
        return None  
    
    citizenship = str(citizenship).strip().upper()
    
    if citizenship == 'ES':
        return None
    
    documents = {
        'idcardno': row.get('idcardno', ''),
        'passportid': row.get('passportid', ''),
        'DRIVERLICENSENO': row.get('DRIVERLICENSENO', ''),
        'personalid': row.get('personalid', '') 
    }
    
    provided_documents = []
    for doc_name, doc_value in documents.items():
        if doc_value and str(doc_value).strip():
            provided_documents.append(doc_name)
    
    if not provided_documents:
        return "Non-residents must provide at least one document: ID card, passport, driver's license, or personal ID"
    
    return None  

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
    

    if re.match(r'^LIMA\d{2}$', zip_code.upper()):
        return None  
    
    if re.match(r'^\d{5}$', zip_code):
        return None  
    
    if re.match(r'^\d{6}$', zip_code):
        return None  
    
    return "Peru zip code must be 5 digits, 6 digits, or LIMA format (LIMAxx)"
