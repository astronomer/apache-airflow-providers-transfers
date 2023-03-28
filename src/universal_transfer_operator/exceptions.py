class DatabaseCustomError(ValueError, AttributeError):
    """
    Inappropriate argument value (of correct type) or attribute
    not found while running query. while running query
    """
