import re
import numpy as np
from typing import List, Dict
from transformers import AutoTokenizer
from nltk.tokenize import sent_tokenize
import nltk

class TextPreprocessor:
    def __init__(self):
        # Initialize tokenizer for chunking
        self.tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
        # Download NLTK data for sentence tokenization
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
    
    def clean_text(self, text: str) -> str:
        """Makes text cleaner by:
        1. Converting to lowercase
        2. Keeping English alphabet, mathematical symbols, and spaces
        3. Preserving common mathematical expressions
        """
        # Step 1: Make all letters lowercase
        cleaned_text = text.lower()
        
        # Step 2: Keep English letters, numbers, math symbols, and spaces
        # Define allowed math symbols
        math_symbols = [
            'a-z0-9\\s',  # Letters, numbers, spaces
            '\\+\\-\\*/=',  # Basic operators
            '<>≤≥≠',      # Comparison operators
            '∑∏∫√∂∞π',    # Advanced math symbols
            '\\(\\)\\[\\]\\{\\}\\^',  # Brackets and power
            '\\.,',       # Dots and commas
        ]
        
        # Join all symbols and create pattern
        pattern = f'[^{"".join(math_symbols)}]'
        cleaned_text = re.sub(pattern, '', cleaned_text)
        
        # Step 3: Replace multiple spaces with single space and trim
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    def is_duplicate(self, text: str, other_texts: List[str]) -> bool:
        """Check if text is too similar to any existing text"""
        if not text or not other_texts:
            return False
            
        # Clean the text first
        text = self.clean_text(text)
        text_chars = set(text)
        
        # Compare with each existing text
        for other in other_texts:
            other = self.clean_text(other)
            other_chars = set(other)
            
            # Calculate similarity ratio
            common_chars = len(text_chars & other_chars)
            total_chars = len(text_chars | other_chars)
            
            # If 80% or more similar, it's a duplicate
            if total_chars > 0 and (common_chars / total_chars) >= 0.8:
                return True
                
        return False
    
    def chunk_text(self, text: str, max_tokens: int = 512) -> List[str]:
        """Split text into chunks of maximum token length"""
        # Tokenize the entire text
        tokens = self.tokenizer.encode(text, add_special_tokens=False)
        chunks = []
        
        # Split into chunks of max_tokens
        for i in range(0, len(tokens), max_tokens):
            chunk_tokens = tokens[i:i + max_tokens]
            chunk_text = self.tokenizer.decode(chunk_tokens)
            chunks.append(chunk_text)
        
        return chunks
    
    def calculate_stats(self, text: str) -> Dict:
        """Calculate basic NLP statistics for the text"""
        # Tokenize into sentences
        sentences = sent_tokenize(text)
        
        # Calculate token counts
        tokens = text.split()
        token_count = len(tokens)
        unique_tokens = len(set(tokens))
        
        # Calculate sentence length statistics
        sentence_lengths = [len(sent.split()) for sent in sentences]
        
        stats = {
            'token_count': token_count,
            'unique_tokens': unique_tokens,
            'sentence_count': len(sentences),
            'avg_sentence_length': np.mean(sentence_lengths),
            'max_sentence_length': max(sentence_lengths),
            'min_sentence_length': min(sentence_lengths),
            'sentence_length_std': np.std(sentence_lengths)
        }
        
        return stats
    
    def process_text(self, text: str, other_texts: List[str] = None) -> Dict:
        """Main function that processes text by:
        1. Converting to lowercase
        2. Keeping English alphabet and mathematical symbols
        3. Checking for duplicates
        4. Chunking text into 512-token segments
        5. Calculating NLP statistics
        """
        # Clean the text
        cleaned_text = self.clean_text(text)
        
        # Check for duplicates if other_texts is provided
        if other_texts and self.is_duplicate(cleaned_text, other_texts):
            return {'text': '', 'chunks': [], 'stats': {}}
        
        # Chunk the text
        chunks = self.chunk_text(cleaned_text)
        
        # Calculate statistics
        stats = self.calculate_stats(cleaned_text)
        
        return {
            'text': cleaned_text,
            'chunks': chunks,
            'stats': stats
        }