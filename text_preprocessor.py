import re
import numpy as np
from typing import List, Dict, Tuple
from collections import Counter
from transformers import AutoTokenizer
from nltk.tokenize import sent_tokenize
import nltk

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

class TextPreprocessor:
    def __init__(self, max_tokens: int = 512, min_chars: int = 50):
        self.max_tokens = max_tokens
        self.min_chars = min_chars
        self.tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
    
    def clean_text(self, text: str) -> str:
        """Basic text cleaning"""
        # Convert to lowercase
        text = text.lower()
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Normalize punctuation
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        return text.strip()
    
    def is_valid_text(self, text: str) -> bool:
        """Check if text meets quality criteria"""
        if len(text) < self.min_chars:
            return False
            
        # Check for gibberish (high consonant ratio)
        consonants = len(re.findall(r'[abcdfghjklmnpqrstvwxyz]', text.lower()))
        total_chars = len(re.findall(r'[a-z]', text.lower()))
        if total_chars > 0 and consonants / total_chars > 0.9:
            return False
            
        return True
    
    def chunk_text(self, text: str) -> List[str]:
        """Split text into chunks of max_tokens"""
        tokens = self.tokenizer.encode(text)
        chunks = []
        
        for i in range(0, len(tokens), self.max_tokens):
            chunk_tokens = tokens[i:i + self.max_tokens]
            chunk_text = self.tokenizer.decode(chunk_tokens, skip_special_tokens=True)
            chunks.append(chunk_text)
            
        return chunks
    
    def calculate_stats(self, text: str) -> Dict:
        """Calculate NLP statistics for the text"""
        sentences = sent_tokenize(text)
        tokens = self.tokenizer.encode(text, add_special_tokens=False)
        unique_tokens = len(set(tokens))
        
        sentence_lengths = [len(self.tokenizer.encode(s)) for s in sentences]
        
        stats = {
            'token_count': len(tokens),
            'unique_tokens': unique_tokens,
            'sentence_count': len(sentences),
            'avg_sentence_length': np.mean(sentence_lengths) if sentence_lengths else 0,
            'max_sentence_length': max(sentence_lengths) if sentence_lengths else 0,
            'min_sentence_length': min(sentence_lengths) if sentence_lengths else 0
        }
        
        return stats
    
    def process_text(self, text: str) -> Tuple[List[str], Dict]:
        """Complete text processing pipeline"""
        # Clean text
        cleaned_text = self.clean_text(text)
        
        # Validate text
        if not self.is_valid_text(cleaned_text):
            return [], {}
        
        # Chunk text
        chunks = self.chunk_text(cleaned_text)
        
        # Calculate statistics
        stats = self.calculate_stats(cleaned_text)
        
        return chunks, stats