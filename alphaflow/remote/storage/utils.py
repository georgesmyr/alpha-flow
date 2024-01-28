def is_iterator_empty(iterator):
    """
    Checks if an iterator is empty.
    :param iterator: The iterator to check.
    :return: True if the iterator is empty, False otherwise.
    """
    try:
        next(iterator)
        return False
    except StopIteration:
        return True
