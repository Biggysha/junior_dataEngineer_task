import re
from typing import Dict, List

class DataValidator:
    def __init__(self, max_non_alphanumeric_ratio: float = 0.3):
        self.max_non_alphanumeric_ratio = max_non_alphanumeric_ratio
        
        # Regex patterns for sensitive information
        self.email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        self.phone_pattern = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
        self.ssn_pattern = re.compile(r'\b\d{3}[-]?\d{2}[-]?\d{4}\b')
        
    def check_non_alphanumeric_ratio(self, text: str) -> bool:
        """Check if the text has acceptable ratio of non-alphanumeric content"""
        if not text:
            return False
            
        total_chars = len(text)
        alphanumeric_chars = len(re.findall(r'[a-zA-Z0-9\s]', text))
        
        if total_chars == 0:
            return False
            
        non_alphanumeric_ratio = 1 - (alphanumeric_chars / total_chars)
        return non_alphanumeric_ratio <= self.max_non_alphanumeric_ratio
    
    def detect_sensitive_info(self, text: str) -> Dict[str, List[str]]:
        """Detect potential sensitive information in text"""
        sensitive_info = {
            'emails': self.email_pattern.findall(text),
            'phone_numbers': self.phone_pattern.findall(text),
            'ssn': self.ssn_pattern.findall(text)
        }
        return sensitive_info
    
    def check_data_duplication(self, text: str, existing_texts: List[str], similarity_threshold: float = 0.8) -> bool:
        """Check if text is too similar to existing texts using character-level similarity"""
        if not text or not existing_texts:
            return False
            
        text_chars = set(text.lower())
        
        for existing_text in existing_texts:
            existing_chars = set(existing_text.lower())
            
            # Calculate Jaccard similarity
            intersection = len(text_chars.intersection(existing_chars))
            union = len(text_chars.union(existing_chars))
            
            if union > 0 and intersection / union >= similarity_threshold:
                return True
                
        return False
    
    def validate_text(self, text: str, existing_texts: List[str] = None) -> Dict:
        """Complete validation of text data"""
        validation_results = {
            'is_valid': True,
            'issues': []
        }
        
        # Check non-alphanumeric ratio
        if not self.check_non_alphanumeric_ratio(text):
            validation_results['is_valid'] = False
            validation_results['issues'].append('High non-alphanumeric content')
        
        # Check for sensitive information
        sensitive_info = self.detect_sensitive_info(text)
        has_sensitive_info = any(info for info in sensitive_info.values())
        if has_sensitive_info:
            validation_results['is_valid'] = False
            validation_results['issues'].append('Contains sensitive information')
            validation_results['sensitive_info'] = sensitive_info
        
        # Check for duplication if existing_texts provided
        if existing_texts and self.check_data_duplication(text, existing_texts):
            validation_results['is_valid'] = False
            validation_results['issues'].append('Possible duplicate content')
        
        return validation_results