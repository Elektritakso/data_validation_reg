
import logging
from typing import Dict, List, Any
from collections import Counter
from validators_common import *
from validator_registry import validator_registry

logger = logging.getLogger(__name__)

def enhance_error_with_value(error_message: str, field_name: str, field_value: Any) -> str:
    """
    Enhance generic error message with actual invalid value
    
    Args:
        error_message: Generic error message from validator
        field_name: Name of the field being validated
        field_value: The actual invalid value
        
    Returns:
        Enhanced error message with the actual value
    """
    if error_message is None:
        return None
    
    field_value_str = str(field_value).strip()
    
    return f"{error_message}: '{field_value_str}'"

def validate_single_row(row_data: Dict[str, Any], row_index: int, 
                       required_columns: List[str], 
                       regulation_info: Dict = None) -> Dict[str, Any]:
    """
    Validate a single row of data
    
    Args:
        row_data: Dictionary containing row data
        row_index: Index of the row (for error reporting)
        required_columns: List of required columns to validate
        regulation_info: Regulation-specific information
        
    Returns:
        Dictionary with validation result for this row
    """
    row_errors = []
    is_valid = True
    code_value = row_data.get('code', 'N/A')
    
    selected_regulation = regulation_info.get('regulation') if regulation_info else None
    required_columns_lower = [col.lower() for col in required_columns]
    
    def should_validate(column_name):
        if column_name is None:
            return False
        return column_name.lower() in required_columns_lower
    
    for field in row_data.keys():
        if field is None or not should_validate(field):
            continue
        
        value = row_data[field]
        field_lower = field.lower() if field is not None else ""
        
        if (field_lower == 'personalid' and 
            selected_regulation and selected_regulation.get('name') == 'Peru'):
            citizenship = row_data.get('citizenship', '')
            if str(citizenship).strip().upper() == 'ES' and not str(value).strip():
                error_msg = f"{field} is required for Spanish residents"
                row_errors.append(f"{code_value} {error_msg}")
                is_valid = False
                continue
            elif not str(value).strip():
                continue
        elif not str(value).strip():
            error_msg = f"{field} is required but missing"
            row_errors.append(f"{code_value} {error_msg}")
            is_valid = False
            continue
        
        error = _validate_field(field_lower, value, row_data, selected_regulation)
        if error:
            enhanced_error = enhance_error_with_value(error, field, value)
            row_errors.append(f"{code_value} {field}: {enhanced_error}")
            is_valid = False
    
    cross_field_errors = _validate_cross_fields(row_data, selected_regulation)
    if cross_field_errors:
        for error in cross_field_errors:
            row_errors.append(f"{code_value} {error}")
            is_valid = False
    
    return {
        'row': row_index + 1,
        'code': code_value,
        'valid': is_valid,
        'errors': row_errors
    }

def _validate_field(field_lower: str, value: Any, row_data: Dict, 
                   selected_regulation: Dict = None) -> str:
    """
    Validate a specific field value
    
    Args:
        field_lower: Field name in lowercase
        value: Field value to validate
        row_data: Complete row data (for context)
        selected_regulation: Regulation information
        
    Returns:
        Error message if invalid, None if valid
    """
    if field_lower == 'email':
        return enhanced_email_validation(value)
    
    elif field_lower == 'birthdate':
        birthdate_error = is_invalid_birthdate(value)
        if birthdate_error:
            return f"Birthdate: {birthdate_error}"
    
    elif field_lower in ['phone', 'cellphone']:
        phone_error = validate_phone_number(value)
        if phone_error:
            return f"{field_lower}: {phone_error}"
    
    elif field_lower == 'firstname':
        name_error = validate_name(value)
        if name_error:
            return name_error
        
        length_error = validate_name_length(value, 1, 50)
        if length_error:
            return length_error
    
    elif field_lower == 'lastname':
        name_error = validate_name(value)
        if name_error:
            return name_error
        
        length_error = validate_name_length(value, 1, 50)
        if length_error:
            return length_error
    
    elif field_lower == 'regioncode':
        regioncode_error = validate_regioncode(value)
        if regioncode_error:
            return f"regioncode: {regioncode_error}"
    
    elif field_lower == 'provincecode':
        provincecode_error = validate_provincecode(value)
        if provincecode_error:
            return f"provincecode: {provincecode_error}"
    
    elif field_lower == 'province':
        province_error = validate_province(value)
        if province_error:
            return f"province: {province_error}"
    
    elif field_lower == 'personalid':
        # Use validator registry for regulation-specific validation
        regulation_name = selected_regulation.get('name') if selected_regulation else None
        personalid_validator = validator_registry.get_validator(regulation_name, 'personalid')
        
        if personalid_validator:
            conditional_info = validator_registry.has_conditional_validation(regulation_name, 'personalid')
            if conditional_info and conditional_info.get('conditional'):
                depends_on = conditional_info.get('depends_on')
                dependency_value = row_data.get(depends_on, '') if depends_on else ''
                personalid_error = personalid_validator(value, dependency_value)
            else:
                personalid_error = personalid_validator(value)
        else:
            personalid_error = validate_personalid(value)
        
        if personalid_error:
            return f"personalid: {personalid_error}"
    
    elif field_lower == 'idcardno':
        idcardno_error = validate_idcardno(value)
        if idcardno_error:
            return f"idcardno: {idcardno_error}"
    
    elif field_lower == 'citizenship':
        citizenship_error = validate_citizenship(value)
        if citizenship_error:
            return f"citizenship: {citizenship_error}"
    
    elif field_lower == 'signupdate':
        signup_date_error = validate_signup_date(value)
        if signup_date_error:
            return f"signupdate: {signup_date_error}"
    
    elif field_lower == 'address':
        crlf_error = check_for_crlf(value)
        if crlf_error:
            return f"address: {crlf_error}"
    
    elif field_lower == 'countrycode':
        country_error = validate_country_code(value)
        if country_error:
            return f"countrycode: {country_error}"
    
    elif field_lower in ['zipcode', 'postalcode', 'zip', 'postcode']:
        regulation_name = selected_regulation.get('name') if selected_regulation else None
        zip_validator = validator_registry.get_validator(regulation_name, 'zip')
        
        if zip_validator:
            zip_error = zip_validator(value)
        else:
            zip_error = validate_zip_code(value)
        
        if zip_error:
            return f"{field_lower}: {zip_error}"
    
    elif field_lower == 'signuplanguagecode':
        lang_error = validate_language_code(value)
        if lang_error:
            return f"signuplanguagecode: {lang_error}"
    
    elif field_lower == 'currencycode':
        currency_error = validate_currency_code(value)
        if currency_error:
            return f"currencycode: {currency_error}"
    
    return None

def _validate_cross_fields(row_data: Dict, selected_regulation: Dict = None) -> List[str]:
    """
    Validate cross-field relationships
    
    Args:
        row_data: Complete row data
        selected_regulation: Regulation information
        
    Returns:
        List of error messages
    """
    errors = []
    
    address_errors = validate_address_fields(row_data)
    errors.extend(address_errors)
    
    language_country_errors = validate_language_country_consistency(row_data)
    errors.extend(language_country_errors)
    
    if selected_regulation:
        regulation_name = selected_regulation.get('name')
        document_validator = validator_registry.get_validator(regulation_name, 'documents')
        if document_validator:
            doc_error = document_validator(row_data)
            if doc_error:
                errors.append(doc_error)
    
    return errors

def find_duplicates_parallel(data_chunk: List[Dict], field_name: str, 
                           start_index: int = 0) -> Dict[str, List[int]]:
    """
    Find duplicate values in a data chunk for a specific field
    
    Args:
        data_chunk: List of row dictionaries
        field_name: Name of field to check for duplicates
        start_index: Starting index for global row numbering
        
    Returns:
        Dictionary mapping values to lists of row indices
    """
    value_indices = {}
    
    for idx, row in enumerate(data_chunk):
        if field_name not in row or not row[field_name]:
            continue
        
        value = str(row[field_name]).strip().lower()
        if not value:
            continue
        
        global_idx = start_index + idx
        
        if value in value_indices:
            value_indices[value].append(global_idx)
        else:
            value_indices[value] = [global_idx]
    
    return {value: indices for value, indices in value_indices.items() if len(indices) > 1}
