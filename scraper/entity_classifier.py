"""
Entity Classifier - Tiered system to distinguish job titles from company names.

Tier 1: Curated company database lookup (fastest, most reliable)
Tier 2: spaCy NER classification (handles unknowns)
Tier 3: Regex-based heuristics (fallback)
"""

import json
import re
from pathlib import Path
from typing import Optional, Tuple
from config import logger

# Lazy load spaCy to avoid import cost if not needed
_nlp = None


def _get_nlp():
    """Lazy load spaCy model, auto-downloading if needed."""
    global _nlp
    if _nlp is None:
        try:
            import spacy
            try:
                _nlp = spacy.load("en_core_web_sm")
                logger.info("✓ spaCy model loaded")
            except OSError:
                # Model not found - try to download it
                logger.info("Downloading spaCy model (one-time setup)...")
                import subprocess
                subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"], 
                              check=True, capture_output=True)
                _nlp = spacy.load("en_core_web_sm")
                logger.info("✓ spaCy model downloaded and loaded")
        except Exception as e:
            logger.warning(f"Could not load spaCy: {e}. Entity classification will use regex fallback.")
            _nlp = False  # Mark as unavailable
    return _nlp if _nlp else None


class EntityClassifier:
    """Classifies text as company, job_title, location, or unknown."""
    
    def __init__(self):
        self.companies_db = set()
        self.universities_db = set()
        self.job_titles_db = set()
        self.aliases = {}
        self._load_company_database()
        
        # Regex patterns (from existing scraper.py)
        self.company_hints = re.compile(
            r'\b(Inc\.?|Corp\.?|LLC|Ltd\.?|Company|Co\.?|Technologies|Solutions|'
            r'Enterprises|Group|Partners|Services|Consulting|Software|Systems|'
            r'Institute|Lab|Laboratory|Foundation|Private Limited|Pvt\.?\s*Ltd\.?)\b',
            re.I
        )
        
        self.title_hints = re.compile(
            r'\b(Engineer|Developer|Manager|Director|Analyst|Designer|Consultant|'
            r'Specialist|Associate|Intern|Lead|Senior|Junior|Sr\.?|Jr\.?|Chief|'
            r'Head|VP|Vice President|Coordinator|Administrator|Representative|'
            r'Officer|Architect|Scientist|Professor|Teacher|Instructor|Tutor|'
            r'Assistant|Student|Trainee|Fellow|Researcher|Technician|Operator|'
            r'Programmer|QA|Quality|Tester|Support|Agent|Staff|Crew|Member|'
            r'Executive|President|CEO|CTO|CFO|COO|CIO|CMO|Recruiter|Talent Acquisition|'
            r'Cashier|Server|Barista|Waiter|Waitress|Host|Hostess|Bartender|Clerk|'
            r'Ambassador|Volunteer|Attendant|Internship|Apprentice|Founder|Co-Founder|'
            r'Owner|Partner|Principal|Managing Director|General Manager|Supervisor|'
            r'Coach|Evaluator|Busser|Assembler|Technician|Banker|Shopper)\b',
            re.I
        )
        
        # Location patterns - common US states, cities, and location keywords
        self.location_patterns = re.compile(
            r'\b(United States|USA|U\.S\.A\.?|India|Canada|UK|United Kingdom|'
            r'Germany|Australia|France|Japan|China|Brazil|Mexico|'
            r'Texas|California|New York|Florida|Illinois|Pennsylvania|Ohio|'
            r'Georgia|Michigan|North Carolina|New Jersey|Virginia|Washington|'
            r'Arizona|Massachusetts|Tennessee|Indiana|Missouri|Maryland|'
            r'Wisconsin|Colorado|Minnesota|South Carolina|Alabama|Louisiana|'
            r'Kentucky|Oregon|Oklahoma|Connecticut|Utah|Iowa|Nevada|Arkansas|'
            r'Mississippi|Kansas|New Mexico|Nebraska|Idaho|West Virginia|Hawaii|'
            r'New Hampshire|Maine|Montana|Rhode Island|Delaware|South Dakota|'
            r'North Dakota|Alaska|Vermont|Wyoming|'
            r'Dallas|Houston|Austin|San Antonio|Fort Worth|El Paso|Arlington|'
            r'Denton|Irving|Plano|Frisco|McKinney|Carrollton|Lewisville|'
            r'Garland|Richardson|Euless|Gainesville|'
            r'Los Angeles|San Francisco|San Diego|San Jose|Seattle|Portland|'
            r'Phoenix|Denver|Chicago|Boston|New York City|NYC|Philadelphia|'
            r'Atlanta|Miami|Orlando|Tampa|Charlotte|Nashville|Memphis|'
            r'Detroit|Minneapolis|Milwaukee|Kansas City|St\\.? Louis|'
            r'Metroplex|Area|Metro|Greater|Region|Remote|Hybrid|On-site)\b',
            re.I
        )
        
        # Pattern for "City, State" or "City, Country" format
        # Excludes patterns with ", LLC", ", Inc" etc.
        self.city_state_pattern = re.compile(
            r'^[A-Z][a-zA-Z\s\-\.]+,\s*[A-Z][a-zA-Z\s]+$'
        )
        
        # Company suffix pattern - these are NOT locations
        self.company_suffix_pattern = re.compile(
            r',\s*(LLC|Inc\.?|Corp\.?|Ltd\.?|Co\.?|Company)$',
            re.I
        )
    
    def _load_company_database(self):
        """Load curated company database from JSON file."""
        db_path = Path(__file__).parent / "data" / "companies.json"
        try:
            if db_path.exists():
                data = json.loads(db_path.read_text(encoding="utf-8"))
                self.companies_db = set(c.lower().strip() for c in data.get("companies", []))
                self.universities_db = set(u.lower().strip() for u in data.get("universities", []))
                self.job_titles_db = set(t.lower().strip() for t in data.get("job_titles", []))
                self.aliases = {k.lower().strip(): v for k, v in data.get("aliases", {}).items()}
                logger.info(f"✓ Loaded {len(self.companies_db)} companies, {len(self.universities_db)} universities, {len(self.job_titles_db)} job titles from database")
        except Exception as e:
            logger.warning(f"Could not load company database: {e}")
    
    def classify(self, text: str) -> Tuple[str, float]:
        """
        Classify text as 'company', 'job_title', 'location', 'university', or 'unknown'.
        
        Returns:
            Tuple of (classification, confidence)
            - confidence: 1.0 = database match, 0.8 = spaCy, 0.5 = regex, 0.3 = guess
        """
        if not text or not text.strip():
            return ("unknown", 0.0)
        
        clean_text = text.strip()
        
        # Quick location check first (to filter out obvious locations)
        if self._is_obvious_location(clean_text):
            return ("location", 0.95)
        
        # --- Tier 1: Database Lookup ---
        result = self._tier1_database_lookup(clean_text)
        if result:
            return result
        
        # --- Tier 2: spaCy NER ---
        result = self._tier2_spacy_ner(clean_text)
        if result:
            return result
        
        # --- Tier 3: Regex Heuristics ---
        return self._tier3_regex_heuristics(clean_text)
    
    def _is_obvious_location(self, text: str) -> bool:
        """Quick check for obvious location patterns."""
        # Exclude company suffixes - ", LLC", ", Inc", etc. are NOT locations
        if self.company_suffix_pattern.search(text):
            return False
        
        # "City, State" or "City, Country" pattern
        if self.city_state_pattern.match(text):
            return True
        return False
    
    def _tier1_database_lookup(self, text: str) -> Optional[Tuple[str, float]]:
        """Check against curated company/university database."""
        text_lower = text.lower().strip()
        
        # Direct company match
        if text_lower in self.companies_db:
            return ("company", 1.0)
        
        # Direct university match
        if text_lower in self.universities_db:
            return ("university", 1.0)
        
        # Direct job title match
        if text_lower in self.job_titles_db:
            return ("job_title", 1.0)
        
        # Alias match
        if text_lower in self.aliases:
            return ("company", 1.0)
        
        # Partial match - company name might be part of longer text
        # e.g., "State Farm · Full-time" should match "State Farm"
        for company in self.companies_db:
            if len(company) >= 4:  # Avoid matching very short strings
                if text_lower.startswith(company) or company in text_lower:
                    return ("company", 0.95)
        
        # Partial match for universities
        for uni in self.universities_db:
            if len(uni) >= 4:
                if text_lower.startswith(uni) or uni in text_lower:
                    return ("university", 0.95)
        
        # Partial match for job titles
        for title in self.job_titles_db:
            if len(title) >= 4:
                if text_lower == title or text_lower.startswith(title + " "):
                    return ("job_title", 0.95)
        
        return None
    
    def _tier2_spacy_ner(self, text: str) -> Optional[Tuple[str, float]]:
        """Use spaCy NER to classify entities."""
        nlp = _get_nlp()
        if not nlp:
            return None
        
        doc = nlp(text)
        
        # Common job title ending words that spaCy sometimes misclassifies as ORG
        job_title_endings = {"engineer", "developer", "manager", "director", "analyst",
                            "designer", "consultant", "specialist", "associate", "intern",
                            "lead", "coordinator", "administrator", "representative",
                            "officer", "architect", "scientist", "professor", "teacher",
                            "instructor", "tutor", "assistant", "student", "trainee",
                            "fellow", "researcher", "technician", "operator", "programmer",
                            "tester", "support", "agent", "staff", "executive", "recruiter",
                            "cashier", "server", "barista", "clerk", "attendant", "member",
                            "volunteer", "ambassador", "supervisor", "owner", "founder", "partner",
                            "coach", "evaluator", "busser", "assembler", "tech", "banker", "tutor",
                            "shopper"}
        
        # Check all entities found
        for ent in doc.ents:
            if ent.label_ == "ORG":
                # Check if this looks like a job title
                # 1. Last word check (e.g. "Software Engineer")
                last_word = text.split()[-1].lower() if text.split() else ""
                
                # Handle slashes/punctuation in last word (e.g. "Co-op/Intern")
                clean_last = last_word.replace('/', ' ').replace('-', ' ').split()[-1] if last_word else ""
                
                if last_word in job_title_endings or clean_last in job_title_endings:
                    return ("job_title", 0.7)  # Override spaCy's ORG classification
                
                # 2. Strong keyword check - if IT CONTAINS "Intern", "Co-op", "Student" it is likely a title
                # even if SpaCy thinks it's an ORG (e.g. "Engineering Intern")
                text_lower = text.lower()
                strong_title_indicators = {"intern", "co-op", "student", "fellow", "trainee", "assistant"}
                if any(ind in text_lower.split() or ind in text_lower.replace('/', ' ').split() for ind in strong_title_indicators):
                     return ("job_title", 0.75)
                
                # 3. Check for strong title prefixes (Director of, Head of, VP of, Chief)
                # "Director of Software Engineering" -> SpaCy calls it ORG, but it's a title
                strong_prefixes = {"director", "head", "vp", "vice president", "chief", "senior", "principal", "lead", "sr", "jr", "sr.", "jr.", "exec", "executive", "managing", "general"}
                first_word = text_lower.split()[0] if text_lower.split() else ""
                if first_word in strong_prefixes:
                    return ("job_title", 0.75)
                
                # Double-check it's not a university (spaCy sometimes tags them as ORG)
                ent_lower = ent.text.lower()
                if any(kw in ent_lower for kw in ["university", "college", "school", "institute"]):
                    return ("university", 0.85)
                return ("company", 0.8)
            elif ent.label_ == "GPE":  # Geo-political entity (cities, states, countries)
                return ("location", 0.9)
            elif ent.label_ == "LOC":  # Location
                return ("location", 0.9)
        
        return None
    
    def _tier3_regex_heuristics(self, text: str) -> Tuple[str, float]:
        """Fall back to regex pattern matching."""
        
        # Check for location patterns
        if self.location_patterns.search(text):
            # If it matches location keywords, likely a location
            if self.city_state_pattern.match(text):
                return ("location", 0.7)
            # Could still be part of a company name, lower confidence
            # e.g., "Texas Instruments" has "Texas" but is a company
        
        has_company_hint = bool(self.company_hints.search(text))
        has_title_hint = bool(self.title_hints.search(text))
        
        # Check for university keywords
        text_lower = text.lower()
        has_university_hint = any(kw in text_lower for kw in ["university", "college", "school of", "institute of"])
        
        if has_university_hint:
            return ("university", 0.6)
        
        # Strong company indicators - these are ONLY found in company names
        strong_company_pattern = re.compile(
            r'\b(Inc\.?|Corp\.?|LLC|Ltd\.?|Company|Co\.|Technologies|Solutions|'
            r'Enterprises|Partners|Consulting|Software|Systems|Private Limited|Pvt\.?\s*Ltd\.?)$',
            re.I
        )
        has_strong_company = bool(strong_company_pattern.search(text))
        
        # Job titles often have seniority prefixes or role suffixes
        # If it looks like "Senior X" or "X II" or "X Intern", it's likely a title
        title_structure = re.compile(
            r'^(Senior|Junior|Lead|Chief|Principal|Staff|Head|VP|Vice President|Sr\.?|Jr\.?|Asst\.?|Exec\.?)\s+\w+|'
            r'\w+\s+(I{1,3}|IV|V|1|2|3)\s*$|'
            r'\w+\s+(Intern|Trainee|Fellow|Assistant|Apprentice)\s*$',
            re.I
        )
        has_title_structure = bool(title_structure.match(text))
        
        if has_strong_company:
            return ("company", 0.95)  # Boost confidence for explicit Legal Entities (LLC, Inc)
        elif has_title_structure:
            return ("job_title", 0.6)
        elif has_company_hint and not has_title_hint:
            return ("company", 0.5)
        elif has_title_hint and not has_company_hint:
            return ("job_title", 0.5)
        elif has_company_hint and has_title_hint:
            # Both match - prefer title if it ends with a title word
            # e.g., "Project Engineer" ends with "Engineer" which is a title
            if text.split()[-1].lower() in ["engineer", "developer", "manager", "analyst", 
                                             "designer", "specialist", "associate", "intern",
                                             "coordinator", "administrator", "architect",
                                             "scientist", "professor", "teacher", "assistant",
                                             "student", "trainee", "researcher", "technician",
                                             "programmer", "tester", "agent", "staff", "officer",
                                             "cashier", "server", "barista", "clerk", "attendant",
                                             "recruiter", "supervisor", "manager", "director", "lead",
                                             "coach", "evaluator", "busser", "assembler", "banker",
                                             "shopper"]:
                return ("job_title", 0.5)
            return ("unknown", 0.3)
        
        return ("unknown", 0.3)
    
    def is_location(self, text: str) -> bool:
        """Quick check if text looks like a location."""
        if not text:
            return False
        
        text_clean = text.strip()
        
        # "City, State/Country" pattern
        if self.city_state_pattern.match(text_clean):
            return True
        
        # Check location keywords with high specificity
        # Only return True if the ENTIRE text is a location, not just contains location words
        text_lower = text_clean.lower()
        pure_locations = [
            "remote", "hybrid", "on-site", "onsite",
            "united states", "usa", "india", "canada", "uk", "germany", "australia",
            "dallas-fort worth metroplex", "greater houston", "bay area"
        ]
        if text_lower in pure_locations:
            return True
        
        # spaCy check - DISABLED
        # "Accela" was being classified as location by spaCy
        # relying on explicit patterns is safer for scraper
        # nlp = _get_nlp()
        # if nlp:
        #     doc = nlp(text_clean)
        #     # If the entire text is recognized as a single GPE/LOC entity
        #     for ent in doc.ents:
        #         if ent.label_ in ("GPE", "LOC") and len(ent.text) >= len(text_clean) * 0.8:
        #             return True
        
        return False
        
        return False
    
    def is_university(self, text: str) -> bool:
        """Check if text is a university/educational institution."""
        if not text:
            return False
        
        text_lower = text.lower().strip()
        
        # Check database
        if text_lower in self.universities_db:
            return True
        
        # Check keywords
        if any(kw in text_lower for kw in ["university", "college", "school of", "institute of technology"]):
            return True
        
        return False


# Singleton instance for easy import
_classifier = None


def get_classifier() -> EntityClassifier:
    """Get singleton classifier instance."""
    global _classifier
    if _classifier is None:
        _classifier = EntityClassifier()
    return _classifier


def classify_entity(text: str) -> Tuple[str, float]:
    """Convenience function to classify a single entity."""
    return get_classifier().classify(text)


def is_location(text: str) -> bool:
    """Convenience function to check if text is a location."""
    return get_classifier().is_location(text)


def is_university(text: str) -> bool:
    """Convenience function to check if text is a university."""
    return get_classifier().is_university(text)
