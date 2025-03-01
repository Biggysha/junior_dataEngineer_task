import re
from typing import Dict, List

class DataValidator:
    def __init__(self):
        # Simple patterns to find sensitive data
        # Pattern for email validation
        self.email_pattern = (
            r'[a-zA-Z0-9._%+-]+' # Username part
            r'@'
            r'[a-zA-Z0-9.-]+' # Domain name
            r'\.[a-zA-Z]{2,}' # Top level domain
        )
        
        # Pattern for phone numbers (XXX-XXX-XXXX or similar)
        self.phone_pattern = (
            r'\b\d{3}' # Area code
            r'[-.]?' # Optional separator
            r'\d{3}' # First three digits
            r'[-.]?' # Optional separator
            r'\d{4}\b' # Last four digits
        )
        
        # Pattern for SSN (XXX-XX-XXXX)
        self.ssn_pattern = (
            r'\b\d{3}' # First three digits
            r'[-]?' # Optional hyphen
            r'\d{2}' # Middle two digits
            r'[-]?' # Optional hyphen
            r'\d{4}\b' # Last four digits
        )
        
    def has_too_many_special_chars(self, text: str) -> bool:
        """Check if text has too many special characters (>30%)"""
        if not text:
            return False
        
        # Count normal vs special characters
        normal_chars = len(re.findall(r'[a-zA-Z0-9\s]', text))
        total_chars = len(text)
        
        if total_chars == 0:
            return False
            
        # If more than 30% special chars, return True
        return (normal_chars / total_chars) < 0.7
    
    def find_private_info(self, text: str) -> Dict[str, List[str]]:
        """Look for emails, phone numbers, and SSNs in text"""
        found_info = {
            'emails': re.findall(self.email_pattern, text),
            'phones': re.findall(self.phone_pattern, text),
            'ssns': re.findall(self.ssn_pattern, text)
        }
        return found_info
    
    def is_duplicate(self, text: str, other_texts: List[str]) -> bool:
        """Check if text is too similar to any existing text"""
        if not text or not other_texts:
            return False
            
        # Convert text to lowercase and get unique characters
        text_chars = set(text.lower())
        
        # Compare with each existing text
        for other in other_texts:
            other_chars = set(other.lower())
            
            # Calculate how similar they are (0 to 1)
            common_chars = len(text_chars & other_chars)
            total_chars = len(text_chars | other_chars)
            
            # If 80% or more similar, it's a duplicate
            if total_chars > 0 and (common_chars / total_chars) >= 0.8:
                return True
                
        return False
    
    def check_text(self, text: str, other_texts: List[str] = None) -> Dict:
        """Check text for problems and return results"""
        result = {
            'is_valid': True,
            'problems': []
        }
        
        # Check for too many special characters
        if self.has_too_many_special_chars(text):
            result['is_valid'] = False
            result['problems'].append('Too many special characters')
        
        # Check for private information
        private_info = self.find_private_info(text)
        if any(info for info in private_info.values()):
            result['is_valid'] = False
            result['problems'].append('Contains private information')
            result['private_info'] = private_info
        
        # Check for duplicates
        if other_texts and self.is_duplicate(text, other_texts):
            result['is_valid'] = False
            result['problems'].append('Similar to existing text')
        
        return result