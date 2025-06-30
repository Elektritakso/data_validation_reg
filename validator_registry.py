import importlib
from validators_common import *
from validators_peru import validate_peru_personalid, validate_peru_documents, validate_peru_zip
from validators_colombia import validate_colombia_personalid
from validators_ims import validate_ims_personalid

class ValidatorRegistry:
    """Registry for managing regulation-specific validators"""
    
    def __init__(self):
        self.regulation_validators = {
            'Peru': {
                'personalid': validate_peru_personalid,
                'documents': validate_peru_documents,
                'zip': validate_peru_zip,
                'conditional_fields': {
                    'personalid': {
                        'conditional': True,
                        'depends_on': 'citizenship'
                    }
                }
            },
            'Colombia': {
                'personalid': validate_colombia_personalid,
            },
            'IMS': {
                'personalid': validate_ims_personalid,
            }
        }
    
    def get_validator(self, regulation_name, validator_type):
        """
        Get a specific validator for a regulation
        
        Args:
            regulation_name: Name of the regulation (e.g., 'Peru', 'Colombia')
            validator_type: Type of validator (e.g., 'personalid', 'documents')
            
        Returns:
            Validator function or None if not found
        """
        regulation_validators = self.regulation_validators.get(regulation_name, {})
        return regulation_validators.get(validator_type)
    
    def has_conditional_validation(self, regulation_name, field_name):
        """
        Check if a field has conditional validation for a regulation
        
        Args:
            regulation_name: Name of the regulation
            field_name: Name of the field
            
        Returns:
            Dictionary with conditional info or None
        """
        regulation_validators = self.regulation_validators.get(regulation_name, {})
        conditional_fields = regulation_validators.get('conditional_fields', {})
        return conditional_fields.get(field_name)
    
    def get_all_validators_for_regulation(self, regulation_name):
        """
        Get all validators for a specific regulation
        
        Args:
            regulation_name: Name of the regulation
            
        Returns:
            Dictionary of all validators for the regulation
        """
        return self.regulation_validators.get(regulation_name, {})
    
    def register_validator(self, regulation_name, validator_type, validator_func):
        """
        Register a new validator for a regulation
        
        Args:
            regulation_name: Name of the regulation
            validator_type: Type of validator
            validator_func: The validator function
        """
        if regulation_name not in self.regulation_validators:
            self.regulation_validators[regulation_name] = {}
        
        self.regulation_validators[regulation_name][validator_type] = validator_func

validator_registry = ValidatorRegistry()
