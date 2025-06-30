
def validate_colombia_personalid(personalid):
    """
    Validate Colombia-specific personal ID requirements
    
    Args:
        personalid: The personal ID value
        
    Returns:
        Error message if invalid, None if valid
    """

    from validators_common import validate_personalid
    return validate_personalid(personalid)

