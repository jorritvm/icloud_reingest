import re

def should_skip_by_partial_match(path, skiplist):
    for keyword in skiplist:
        if keyword.lower() in path.lower():
            return True
    return False


def extract_year_from_path(path):
    # Split the path into components and look for a 4-digit year starting with 20
    parts = re.split(r'[\\/]', path)
    for part in parts:
        match = re.match(r'(20\d{2})', part)
        if match:
            return match.group(1)
    return None


